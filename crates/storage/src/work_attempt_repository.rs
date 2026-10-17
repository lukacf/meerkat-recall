//! Authoritative durable attempt generations. Queue state and lease authority
//! change in one transaction; retry counters and archives never reset fencing.

use chrono::{DateTime, Utc};
use elephant_types::publication::AttemptFence;
use elephant_types::records::{WorkItem, WorkItemStatus};
use elephant_types::RecordId;
use serde::{Deserialize, Serialize};
use std::fmt::Write as _;

use crate::work_item_repository::{WorkItemDb, WORK_ITEM_PROJECTION};
use crate::Repository;
use crate::{EmbeddedMutation, StorageClient, StorageError, StorageResult};

/// Durable authority table, retained across queue retries and archival.
pub const ATTEMPT_TABLE: &str = "work_attempt";
/// Original claim envelope carried by one worker attempt.
pub const ATTEMPT_PAYLOAD_KEY: &str = "_attempt_fence";

/// Persistence representation, with no erased source payload or input content.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkAttempt {
    /// Stable control-row identity derived from the work-item UUID.
    pub id: String,
    /// Named space owning this attempt.
    pub space_id: String,
    /// Queue item whose lease this row authorizes.
    pub work_item_id: RecordId,
    /// Monotonic claim generation, independent of the retry counter.
    pub generation: u64,
    /// Fresh opaque ownership token allocated by the claim transaction.
    pub lease_token: String,
    /// Queue status synchronized with the work item.
    pub status: String,
    /// Deadline after which this generation cannot publish or renew.
    pub lease_expires_at: Option<DateTime<Utc>>,
    /// Conflict marker touched inside transactions that check this authority.
    pub commit_nonce: String,
}

/// Native ID to bind as `$attempt_id` when checking canonical publication.
#[must_use]
pub fn attempt_id(id: &RecordId) -> surrealdb::sql::Thing {
    surrealdb::sql::Thing::from((ATTEMPT_TABLE, surrealdb::sql::Id::Uuid(id.id().into())))
}

fn embedded_id(id: &RecordId) -> String {
    format!("{ATTEMPT_TABLE}:{}", id.id())
}

/// Captures only the authority returned by the original successful claim.
/// Fetching a later claim to construct a fence is never a valid recovery action.
///
/// # Errors
/// Rejects missing, malformed, cross-space, or non-Running claim envelopes.
pub fn fence_from_claim(item: &WorkItem) -> StorageResult<AttemptFence> {
    let value = item.payload.get(ATTEMPT_PAYLOAD_KEY).ok_or_else(|| {
        StorageError::DataIntegrityError("Claim is missing durable attempt fence".to_string())
    })?;
    let fence: AttemptFence = serde_json::from_value(value.clone())
        .map_err(|error| StorageError::DataIntegrityError(error.to_string()))?;
    if fence.space_id != item.base.space_id
        || fence.work_item_id != item.base.id
        || item.status != WorkItemStatus::Running
        || item.leased_by.as_deref() != Some(fence.lease_token.as_str())
    {
        return Err(StorageError::Conflict(
            "Claim attempt fence mismatch".to_string(),
        ));
    }
    Ok(fence)
}

/// Read persisted authority inside an existing transaction without granting a lease.
///
/// # Errors
/// Returns storage or malformed authority-row errors.
pub fn read_attempt(
    tx: &EmbeddedMutation<'_>,
    id: &RecordId,
) -> StorageResult<Option<WorkAttempt>> {
    tx.scan_extension_rows(ATTEMPT_TABLE)?
        .into_iter()
        .find(|(key, _)| key == &embedded_id(id))
        .map(|(_, value)| {
            serde_json::from_value(value)
                .map_err(|error| StorageError::DataIntegrityError(error.to_string()))
        })
        .transpose()
}

fn write_attempt(tx: &mut EmbeddedMutation<'_>, row: &WorkAttempt) -> StorageResult<()> {
    tx.upsert_extension_row(
        ATTEMPT_TABLE,
        serde_json::to_value(row)
            .map_err(|error| StorageError::SerializationError(error.to_string()))?,
    )?;
    Ok(())
}

/// Checks and touches attempt authority inside the SAME embedded transaction as
/// canonical effects. The write forces concurrent renewal/recovery to conflict.
///
/// # Errors
/// Rejects missing, expired, or replaced authority and propagates storage errors.
pub fn check_attempt_fence(
    tx: &mut EmbeddedMutation<'_>,
    fence: &AttemptFence,
    now: DateTime<Utc>,
) -> StorageResult<()> {
    let stale = || {
        StorageError::Conflict(
            "stale_attempt: publication ownership expired or changed".to_string(),
        )
    };
    let mut row = read_attempt(tx, &fence.work_item_id)?.ok_or_else(stale)?;
    let item = tx
        .get::<WorkItem>(&fence.space_id, &fence.work_item_id, false)?
        .ok_or_else(stale)?;
    if row.space_id != fence.space_id
        || row.work_item_id != fence.work_item_id
        || row.generation != fence.generation
        || row.lease_token != fence.lease_token
        || row.status != "running"
        || row.lease_expires_at.is_none_or(|expires| expires <= now)
        || item.status != WorkItemStatus::Running
        || item.leased_by.as_deref() != Some(fence.lease_token.as_str())
        || item.lease_expires_at.is_none_or(|expires| expires <= now)
    {
        return Err(stale());
    }
    row.commit_nonce = uuid::Uuid::new_v4().to_string();
    write_attempt(tx, &row)
}

/// Include inside the canonical BEGIN/COMMIT.
///
/// Required bindings: attempt_id
/// (attempt_id helper), attempt_work_item (native Thing), attempt_space,
/// attempt_generation, attempt_token. This statement MUST NOT run separately
/// before the effects transaction.
pub const ATTEMPT_FENCE_SQL: &str = "
 LET $fenced_attempt = SELECT * FROM ONLY $attempt_id;
 LET $fenced_work = SELECT * FROM ONLY $attempt_work_item;
 IF $fenced_attempt IS NONE OR $fenced_attempt.space_id != $attempt_space
 OR $fenced_attempt.work_item_id != $attempt_work_item
 OR $fenced_attempt.generation != $attempt_generation OR $fenced_attempt.lease_token != $attempt_token
 OR $fenced_attempt.status != 'running' OR $fenced_attempt.lease_expires_at IS NONE
 OR $fenced_attempt.lease_expires_at <= time::now()
 OR $fenced_work IS NONE OR $fenced_work.space_id != $attempt_space
 OR $fenced_work.deleted_at IS NOT NONE OR $fenced_work.status != 'running'
 OR $fenced_work.leased_by != $attempt_token OR $fenced_work.lease_expires_at IS NONE
 OR $fenced_work.lease_expires_at <= time::now() { THROW 'stale_attempt'; };
 UPDATE $attempt_id SET commit_nonce = <string>rand::uuid();
";

/// Adds the native fence check to an existing canonical transaction builder.
/// The caller owns BEGIN/COMMIT and must append effects before COMMIT.
///
/// # Errors
/// Rejects zero or unrepresentable generations and empty ownership tokens.
pub fn append_attempt_fence(
    sql: &mut String,
    bindings: &mut std::collections::BTreeMap<String, surrealdb::sql::Value>,
    fence: &AttemptFence,
) -> StorageResult<()> {
    if fence.generation == 0 || fence.generation > i64::MAX as u64 || fence.lease_token.is_empty() {
        return Err(StorageError::DataIntegrityError(
            "Invalid captured attempt fence".to_string(),
        ));
    }
    bindings.extend(crate::bindings! {
        "attempt_id" => attempt_id(&fence.work_item_id),
        "attempt_work_item" => crate::surreal::thing_from_record_id(&fence.work_item_id),
        "attempt_space" => fence.space_id.clone(), "attempt_generation" => fence.generation,
        "attempt_token" => fence.lease_token.clone(),
    });
    sql.push_str(ATTEMPT_FENCE_SQL);
    Ok(())
}

pub(crate) async fn claim(
    client: &StorageClient,
    space: &str,
    id: &RecordId,
    worker: &str,
    expires: DateTime<Utc>,
) -> StorageResult<Option<WorkItem>> {
    claim_observed(client, space, id, worker, expires, None).await
}

fn observed_unchanged(current: &WorkItem, observed: &WorkItem) -> bool {
    current.base.id == observed.base.id
        && current.base.space_id == observed.base.space_id
        && current.base.updated_at == observed.base.updated_at
        && current.base.created_by == observed.base.created_by
        && current.base.provenance == observed.base.provenance
        && crate::lifecycle_guard::base_envelope_unchanged(&current.base, &observed.base)
        && current.kind == observed.kind
        && current.payload == observed.payload
        && current.idempotency_key == observed.idempotency_key
}

fn claim_observed_tx(
    tx: &mut EmbeddedMutation<'_>,
    space: &str,
    id: &RecordId,
    token: String,
    expires: DateTime<Utc>,
    observed: Option<&WorkItem>,
) -> StorageResult<Option<WorkItem>> {
    let now = Utc::now();
    let Some(mut item) = tx.get::<WorkItem>(space, id, false)? else {
        return Ok(None);
    };
    if observed.is_some_and(|old| !observed_unchanged(&item, old))
        || item.status != WorkItemStatus::Runnable
        || item.run_after.is_some_and(|after| after > now)
        || expires <= now
    {
        return Ok(None);
    }
    if !crate::work_producer_repository::may_claim_or_recover_tx(tx, &item)? {
        return Ok(None);
    }
    let prior = read_attempt(tx, id)?;
    if prior
        .as_ref()
        .is_some_and(|prior| prior.space_id != space || prior.work_item_id != *id)
    {
        return Err(StorageError::DataIntegrityError(
            "Attempt space mismatch".to_string(),
        ));
    }
    let generation = prior
        .map_or(0, |row| row.generation)
        .checked_add(1)
        .filter(|value| i64::try_from(*value).is_ok())
        .ok_or_else(|| {
            StorageError::DataIntegrityError("Attempt generation exhausted".to_string())
        })?;
    let fence = AttemptFence {
        space_id: space.to_string(),
        work_item_id: id.clone(),
        generation,
        lease_token: token.clone(),
    };
    let payload = item.payload.as_object_mut().ok_or_else(|| {
        StorageError::DataIntegrityError("Work payload must be an object".to_string())
    })?;
    payload.insert(
        ATTEMPT_PAYLOAD_KEY.to_string(),
        serde_json::to_value(&fence)
            .map_err(|error| StorageError::SerializationError(error.to_string()))?,
    );
    item.status = WorkItemStatus::Running;
    item.leased_by = Some(token.clone());
    item.lease_expires_at = Some(expires);
    item.attempts = item
        .attempts
        .checked_add(1)
        .ok_or_else(|| StorageError::DataIntegrityError("Retry counter exhausted".to_string()))?;
    item.base.updated_at = now;
    write_attempt(
        tx,
        &WorkAttempt {
            id: embedded_id(id),
            space_id: space.to_string(),
            work_item_id: id.clone(),
            generation,
            lease_token: token,
            status: "running".to_string(),
            lease_expires_at: Some(expires),
            commit_nonce: String::new(),
        },
    )?;
    Ok(Some(tx.replace(item)?))
}

pub(crate) async fn claim_observed(
    client: &StorageClient,
    space: &str,
    id: &RecordId,
    worker: &str,
    expires: DateTime<Utc>,
    observed: Option<&WorkItem>,
) -> StorageResult<Option<WorkItem>> {
    let token = format!("{worker}:{}", uuid::Uuid::new_v4());
    if let Some(store) = client.embedded_store() {
        return store
            .mutate(|tx| claim_observed_tx(tx, space, id, token, expires, observed))
            .await;
    }
    let Some(original) = crate::WorkItemRepository::new(client.clone())
        .get(space, id)
        .await?
    else {
        return Ok(None);
    };
    if observed.is_some_and(|old| !observed_unchanged(&original, old)) {
        return Ok(None);
    }
    let mut sql = "BEGIN TRANSACTION;\n".to_string();
    let mut bindings = crate::bindings! {
        "id" => crate::surreal::thing_from_record_id(id), "id_string" => id.to_string(),
        "attempt_id" => attempt_id(id), "space" => space, "work_lease_token" => token,
        "expires" => crate::surreal::datetime_from_chrono(expires),
        "observed" => crate::canonical_commit::native_expected_fields(crate::WorkItemRepository::work_item_to_value(&original)?),
    };
    if !crate::work_producer_repository::append_claim_or_recovery_guard(
        client,
        &mut sql,
        &mut bindings,
        "claim",
        &original,
    )
    .await?
    {
        return Ok(None);
    }
    let _ = write!(sql, "
        LET $item = SELECT * FROM ONLY $id;
        LET $prior = SELECT * FROM ONLY $attempt_id;
        LET $updated = IF $item IS NOT NONE AND $item.space_id = $space AND $item.deleted_at IS NONE AND $item.status = 'runnable' AND ($item.run_after IS NONE OR $item.run_after <= time::now()) AND $expires > time::now() AND $item.updated_at = $observed.updated_at AND $item.security = $observed.security AND $item.subjects = $observed.subjects AND $item.subjects_state = $observed.subjects_state AND $item.created_by = $observed.created_by AND $item.provenance = $observed.provenance AND $item.kind = $observed.kind AND $item.payload = $observed.payload AND $item.idempotency_key = $observed.idempotency_key {{
          IF $prior IS NOT NONE AND ($prior.space_id != $space OR $prior.work_item_id != $id) {{ THROW 'attempt_space_mismatch'; }};
          LET $generation = IF $prior IS NONE {{ 1 }} ELSE {{ $prior.generation + 1 }};
          IF $generation < 1 OR $item.attempts >= 4294967295 {{ THROW 'attempt_counter_exhausted'; }};
          UPSERT $attempt_id SET space_id = $space, work_item_id = $id, generation = $generation, lease_token = $work_lease_token, status = 'running', lease_expires_at = $expires, commit_nonce = '';
          UPDATE $id SET status = 'running', leased_by = $work_lease_token, lease_expires_at = $expires, attempts += 1, updated_at = time::now(), payload._attempt_fence = {{ space_id: $space, work_item_id: $id_string, generation: $generation, lease_token: $work_lease_token }};
          RETURN (SELECT {WORK_ITEM_PROJECTION} FROM $id);
        }} ELSE {{ RETURN []; }};
        RETURN $updated; COMMIT TRANSACTION;");
    let mut response = client.query_bind_checked(&sql, bindings).await?;
    let rows: Vec<WorkItemDb> = response
        .take(response.num_statements() - 1)
        .map_err(|error| {
            StorageError::DataIntegrityError(format!("Invalid claim result: {error}"))
        })?;
    rows.into_iter().next().map(WorkItem::try_from).transpose()
}

/// All owned transitions require the exact captured token and unexpired
/// authority. A missing attempt is invalid, never invented from the work row.
#[allow(
    clippy::too_many_arguments,
    reason = "Preserve the explicit work identity, original generation/lease and terminal transition contract shared with queue callers."
)]
pub(crate) async fn transition(
    client: &StorageClient,
    space: &str,
    id: &RecordId,
    token: &str,
    status: WorkItemStatus,
    expires: Option<DateTime<Utc>>,
    run_after: Option<DateTime<Utc>>,
    error: Option<String>,
    refund_retry: bool,
) -> StorageResult<Option<WorkItem>> {
    transition_observed(
        client,
        space,
        id,
        token,
        status,
        expires,
        run_after,
        error,
        refund_retry,
        None,
    )
    .await
}

#[allow(
    clippy::too_many_arguments,
    reason = "Preserve the same original attempt fields plus its optional exact observed work image through the fenced transition."
)]
pub(crate) async fn transition_observed(
    client: &StorageClient,
    space: &str,
    id: &RecordId,
    token: &str,
    status: WorkItemStatus,
    expires: Option<DateTime<Utc>>,
    run_after: Option<DateTime<Utc>>,
    error: Option<String>,
    refund_retry: bool,
    observed: Option<&WorkItem>,
) -> StorageResult<Option<WorkItem>> {
    if let Some(store) = client.embedded_store() {
        return store
            .mutate(|tx| {
                let now = Utc::now();
                let Some(mut item) = tx.get::<WorkItem>(space, id, false)? else {
                    return Ok(None);
                };
                let Some(mut row) = read_attempt(tx, id)? else {
                    return Ok(None);
                };
                if observed.is_some_and(|old| !observed_unchanged(&item, old))
                    || item.status != WorkItemStatus::Running
                    || item.leased_by.as_deref() != Some(token)
                    || item.lease_expires_at.is_none_or(|until| until <= now)
                    || row.space_id != space
                    || row.work_item_id != *id
                    || row.lease_token != token
                    || row.status != "running"
                    || row.lease_expires_at.is_none_or(|until| until <= now)
                {
                    return Ok(None);
                }
                let renewing = status == WorkItemStatus::Running;
                if renewing && expires.is_none_or(|until| until <= now) {
                    return Err(StorageError::DataIntegrityError(
                        "Lease renewal requires a future expiry".to_string(),
                    ));
                }
                item.status = status;
                if refund_retry {
                    item.attempts = item.attempts.saturating_sub(1);
                }
                item.leased_by = renewing.then(|| token.to_string());
                item.lease_expires_at = expires;
                item.run_after = run_after;
                item.last_error = error;
                item.base.updated_at = now;
                row.status = status.to_string();
                row.lease_expires_at = expires;
                write_attempt(tx, &row)?;
                Ok(Some(tx.replace(item)?))
            })
            .await;
    }
    let sql = format!("BEGIN TRANSACTION;
      LET $item = SELECT * FROM ONLY $id;
      LET $attempt = SELECT * FROM ONLY $attempt_id;
      LET $updated = IF $item IS NOT NONE AND $item.space_id = $space AND $item.deleted_at IS NONE AND $item.status = 'running' AND $item.leased_by = $work_lease_token AND $item.lease_expires_at > time::now()
      AND $attempt IS NOT NONE AND $attempt.space_id = $space AND $attempt.work_item_id = $id AND $attempt.status = 'running' AND $attempt.lease_token = $work_lease_token AND $attempt.lease_expires_at > time::now() AND ($observed = NONE OR ($item.updated_at = $observed.updated_at AND $item.security = $observed.security AND $item.subjects = $observed.subjects AND $item.subjects_state = $observed.subjects_state AND $item.created_by = $observed.created_by AND $item.provenance = $observed.provenance AND $item.kind = $observed.kind AND $item.payload = $observed.payload AND $item.idempotency_key = $observed.idempotency_key)) {{
        IF $status = 'running' AND ($expires IS NONE OR $expires <= time::now()) {{ THROW 'invalid_lease_expiry'; }};
        UPDATE $attempt_id SET status = $status, lease_expires_at = $expires;
        UPDATE $id SET attempts = IF $refund_retry {{ math::max([attempts - 1, 0]) }} ELSE {{ attempts }}, status = $status, leased_by = IF $status = 'running' {{ $work_lease_token }} ELSE {{ NONE }}, lease_expires_at = $expires, run_after = $run_after, last_error = $error, updated_at = time::now();
        RETURN (SELECT {WORK_ITEM_PROJECTION} FROM $id);
      }} ELSE {{ RETURN []; }};
      RETURN $updated; COMMIT TRANSACTION;");
    let optional_time = |time: Option<DateTime<Utc>>| {
        time.map_or(surrealdb::sql::Value::None, |time| {
            crate::surreal::datetime_from_chrono(time).into()
        })
    };
    let mut response = client.query_bind_checked(&sql, crate::bindings! {
        "id" => crate::surreal::thing_from_record_id(id), "attempt_id" => attempt_id(id), "space" => space,
        "work_lease_token" => token, "status" => status.to_string(), "expires" => optional_time(expires),
        "run_after" => optional_time(run_after), "error" => error, "refund_retry" => refund_retry,
        "observed" => observed.map(crate::WorkItemRepository::work_item_to_value).transpose()?.map_or(surrealdb::sql::Value::None, crate::canonical_commit::native_expected_fields),
    }).await?;
    let rows: Vec<WorkItemDb> = response
        .take(response.num_statements() - 1)
        .map_err(|error| {
            StorageError::DataIntegrityError(format!("Invalid attempt transition: {error}"))
        })?;
    rows.into_iter().next().map(WorkItem::try_from).transpose()
}

/// Expired malformed work can predate an attempt row; recover it without
/// inventing a generation. The next real claim creates generation one.
pub(crate) fn recover_embedded(
    tx: &mut EmbeddedMutation<'_>,
    mut item: WorkItem,
    now: DateTime<Utc>,
) -> StorageResult<WorkItem> {
    if let Some(mut row) = read_attempt(tx, &item.base.id)? {
        row.status = "runnable".to_string();
        row.lease_expires_at = None;
        write_attempt(tx, &row)?;
    }
    item.status = WorkItemStatus::Runnable;
    item.leased_by = None;
    item.lease_expires_at = None;
    item.base.updated_at = now;
    item.last_error = Some("lease_expired: recovered abandoned attempt".to_string());
    tx.replace(item).map_err(Into::into)
}

#[cfg(test)]
#[path = "work_attempt_authorization_tests.rs"]
mod authorization_tests;
