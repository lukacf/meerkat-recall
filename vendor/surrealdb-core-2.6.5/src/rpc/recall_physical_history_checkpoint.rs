//! Typed source-history authority for a physical-generation checkpoint.
use super::recall_physical_checkpoint::{authorize, MAX_TIMEOUT_MS};
use crate::dbs::Session;
use crate::err::Error;
use crate::kvs::{Datastore, RecallHistoryAuthority};
use crate::rpc::{Method, RpcError};
use crate::sql::{Array, Number, Object, Value};
use std::time::Duration;
use trice::Instant;

pub const PROTOCOL: &str = "meerkat-recall-physical-history-checkpoint/v1";
pub const MAX_AUTHORITIES: usize = 4096;
pub const MAX_NATIVE_PAYLOAD_BYTES: usize = 64 * 1024 * 1024;

fn invalid() -> Error {
    Error::Ds("Invalid Recall physical history authority scope, shape or size".into())
}

// Count exact native revision bytes without retaining a second secret-bearing
// buffer. The authority owner validates its closed schema independently.
#[doc(hidden)]
pub fn native_size(value: &Value) -> Result<usize, Error> {
    use revision::Revisioned;
    struct Counter(usize);
    impl std::io::Write for Counter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0 = self
                .0
                .checked_add(bytes.len())
                .filter(|size| *size <= MAX_NATIVE_PAYLOAD_BYTES)
                .ok_or_else(|| std::io::Error::other("Recall native payload exceeds bound"))?;
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut counter = Counter(0);
    value
        .serialize_revisioned(&mut counter)
        .map_err(|_| invalid())?;
    Ok(counter.0)
}

fn check_values(values: &Array) -> Result<(), Error> {
    if values.is_empty() || values.len() > MAX_AUTHORITIES {
        return Err(invalid());
    }
    let mut total = 0_usize;
    for value in values.iter() {
        total = total.checked_add(native_size(value)?).ok_or_else(invalid)?;
        if total > MAX_NATIVE_PAYLOAD_BYTES {
            return Err(invalid());
        }
    }
    Ok(())
}

/// Shared SDK preflight; original journal authentication remains the caller's job.
pub fn validate_authorities(
    namespace: &str,
    database: &str,
    authorities: &[RecallHistoryAuthority],
) -> Result<(), Error> {
    if namespace.is_empty()
        || database.is_empty()
        || authorities.is_empty()
        || authorities.len() > MAX_AUTHORITIES
    {
        return Err(invalid());
    }
    let mut total = 0_usize;
    for authority in authorities {
        authority.validate_bound()?;
        if authority.namespace() != namespace || authority.database() != database {
            return Err(invalid());
        }
        total = total
            .checked_add(native_size(&authority.to_rpc_value())?)
            .ok_or_else(invalid)?;
        if total > MAX_NATIVE_PAYLOAD_BYTES {
            return Err(invalid());
        }
    }
    Ok(())
}

async fn checkpoint_inner(
    kvs: &Datastore,
    session: &Session,
    params: Array,
    remote: bool,
) -> Result<Value, RpcError> {
    let started = Instant::now();
    let (ns, db) = authorize(
        kvs,
        session,
        Method::RecallPhysicalHistoryCheckpoint,
        remote,
    )?;
    let payload = Value::Array(params);
    native_size(&payload)?;
    let Value::Array(params) = &payload else {
        return Err(RpcError::InvalidParams);
    };
    let [Value::Strand(protocol), Value::Strand(expected_ns), Value::Strand(expected_db), Value::Number(Number::Int(timeout_ms)), Value::Array(values)] =
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
    check_values(values)?;
    let authorities = values
        .iter()
        .cloned()
        .map(RecallHistoryAuthority::from_rpc_value)
        .collect::<Result<Vec<_>, _>>()?;
    validate_authorities(&ns, &db, &authorities)?;
    let authority_count = authorities.len();
    let budget = Duration::from_millis(timeout_ms);
    let remaining_ms = u64::try_from(budget.saturating_sub(started.elapsed()).as_millis())
        .map_err(|_| Error::QueryTimedout)?;
    if remaining_ms == 0 {
        return Err(Error::QueryTimedout.into());
    }
    // No ordinary query transaction, no global/key-wide history cut. The engine
    // owns admission, complete version selection and whole-generation retirement.
    kvs.recall_physical_checkpoint_with_history(remaining_ms, authorities)
        .await?;
    if started.elapsed() >= budget {
        return Err(Error::QueryTimedout.into());
    }
    let mut fields = Object::default();
    fields.insert("protocol".into(), PROTOCOL.into());
    fields.insert("namespace".into(), ns.into());
    fields.insert("database".into(), db.into());
    fields.insert("max_timeout_ms".into(), MAX_TIMEOUT_MS.into());
    fields.insert(
        "authority_count".into(),
        Value::from(authority_count as u64),
    );
    fields.insert("complete".into(), true.into());
    Ok(fields.into())
}

/// Authenticated remote administrative checkpoint.
pub async fn checkpoint(
    kvs: &Datastore,
    session: &Session,
    params: Array,
) -> Result<Value, RpcError> {
    checkpoint_inner(kvs, session, params, true).await
}

/// Embedded counterpart retaining trusted anonymous access with auth disabled.
pub async fn local_checkpoint(
    kvs: &Datastore,
    session: &Session,
    params: Array,
) -> Result<Value, RpcError> {
    checkpoint_inner(kvs, session, params, false).await
}
