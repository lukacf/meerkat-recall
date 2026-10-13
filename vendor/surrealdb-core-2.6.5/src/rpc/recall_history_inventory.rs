//! Complete, bounded source-history inspection for the trusted erasure controller.
use super::recall_physical_checkpoint::{authorize, MAX_TIMEOUT_MS};
use super::recall_physical_history_checkpoint::native_size;
use crate::dbs::Session;
use crate::err::Error;
use crate::kvs::Datastore;
use crate::rpc::{Method, RpcError};
use crate::sql::{Array, Number, Object, Value};
use std::time::Duration;
use trice::Instant;

pub const PROTOCOL: &str = "meerkat-recall-source-history-inventory/v1";

/// Recognize the sensitive response envelope before Debug traverses its records.
pub fn is_sensitive_response(value: &Value) -> bool {
    matches!(value, Value::Object(fields)
        if fields.contains_key("snapshot")
            || (fields.contains_key("records") && fields.contains_key("fingerprint"))
            || matches!(fields.get("protocol"), Some(Value::Strand(protocol)) if protocol.0 == PROTOCOL))
}

async fn inventory_inner(
    kvs: &Datastore,
    session: &Session,
    params: Array,
    remote: bool,
) -> Result<Value, RpcError> {
    let started = Instant::now();
    let (ns, db) = authorize(kvs, session, Method::RecallHistoryInventory, remote)?;
    let [Value::Strand(protocol), Value::Strand(expected_ns), Value::Strand(expected_db), Value::Number(Number::Int(timeout_ms))] =
        params.as_slice()
    else {
        return Err(RpcError::InvalidParams);
    };
    if protocol.0 != PROTOCOL || expected_ns.0 != ns || expected_db.0 != db {
        return Err(RpcError::InvalidParams);
    }
    let timeout_ms = u64::try_from(*timeout_ms).map_err(|_| RpcError::InvalidParams)?;
    if !(1..=MAX_TIMEOUT_MS).contains(&timeout_ms) {
        return Err(RpcError::InvalidParams);
    }
    let budget = Duration::from_millis(timeout_ms);
    let remaining_ms = u64::try_from(budget.saturating_sub(started.elapsed()).as_millis())
        .map_err(|_| Error::QueryTimedout)?;
    if remaining_ms == 0 {
        return Err(Error::QueryTimedout.into());
    }
    let snapshot = kvs.recall_history_inventory(remaining_ms, &ns, &db).await?;
    snapshot.verify()?;
    if snapshot.namespace != ns || snapshot.database != db {
        return Err(Error::Ds("Recall history inventory scope differs".into()).into());
    }
    let mut fields = Object::default();
    fields.insert("protocol".into(), PROTOCOL.into());
    fields.insert("namespace".into(), ns.into());
    fields.insert("database".into(), db.into());
    fields.insert("complete".into(), true.into());
    fields.insert("snapshot".into(), snapshot.to_rpc_value());
    let value = Value::Object(fields);
    native_size(&value)?;
    if started.elapsed() >= budget {
        return Err(Error::QueryTimedout.into());
    }
    Ok(value)
}

/// Inspect for a nonexpired root owner in the original selected scope.
pub async fn inventory(
    kvs: &Datastore,
    session: &Session,
    params: Array,
) -> Result<Value, RpcError> {
    inventory_inner(kvs, session, params, true).await
}

/// Trusted embedded counterpart. No ordinary SQL query or history flag is used.
pub async fn local_inventory(
    kvs: &Datastore,
    session: &Session,
    params: Array,
) -> Result<Value, RpcError> {
    inventory_inner(kvs, session, params, false).await
}
