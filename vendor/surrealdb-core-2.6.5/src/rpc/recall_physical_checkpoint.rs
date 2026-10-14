//! Recall's versioned administrative checkpoint, outside ordinary query transactions.
//!
//! The engine owns the checkpoint and its admission barrier. A transport error or
//! cancelled request is never evidence that the physical checkpoint completed.

use crate::dbs::capabilities::MethodTarget;
use crate::dbs::Session;
use crate::err::Error;
use crate::iam::{check::check_ns_db, Action, ResourceKind};
use crate::kvs::Datastore;
use crate::rpc::{Method, RpcError};
use crate::sql::{Array, Number, Object, Value};

pub const PROTOCOL: &str = "meerkat-recall-physical-checkpoint/v1";
pub const MAX_TIMEOUT_MS: u64 = 300_000;

pub(super) fn authorize(
    kvs: &Datastore,
    session: &Session,
    method: Method,
    remote: bool,
) -> Result<(String, String), RpcError> {
    if !kvs.allows_rpc_method(&MethodTarget { method }) {
        return Err(RpcError::MethodNotAllowed);
    }
    // Compaction affects the entire engine. Database/namespace owners must not
    // gain engine-wide administration through their selected namespace/database.
    let trusted_embedded = !remote && !kvs.is_auth_enabled() && session.au.is_anon();
    if !trusted_embedded && !(session.au.is_root() && session.au.has_owner_role()) {
        return Err(RpcError::MethodNotAllowed);
    }
    // This also rejects expired sessions and restricted root credentials.
    kvs.check(session, Action::Edit, ResourceKind::Any.on_root())?;
    let (ns, db) = check_ns_db(session)?;
    if ns.is_empty() || db.is_empty() {
        return Err(RpcError::InvalidParams);
    }
    Ok((ns, db))
}

fn response(ns: String, db: String, complete: bool) -> Value {
    let mut fields = Object::default();
    fields.insert("protocol".into(), Value::from(PROTOCOL));
    fields.insert("namespace".into(), Value::from(ns));
    fields.insert("database".into(), Value::from(db));
    fields.insert("max_timeout_ms".into(), Value::from(MAX_TIMEOUT_MS));
    if complete {
        fields.insert("complete".into(), Value::Bool(true));
    }
    Value::Object(fields)
}

fn capabilities_inner(
    kvs: &Datastore,
    session: &Session,
    params: Array,
    remote: bool,
) -> Result<Value, RpcError> {
    let (ns, db) = authorize(
        kvs,
        session,
        Method::RecallPhysicalCheckpointCapabilities,
        remote,
    )?;
    if !matches!(params.as_slice(), [Value::Strand(protocol)] if protocol.0 == PROTOCOL) {
        return Err(RpcError::InvalidParams);
    }
    if !kvs.recall_physical_checkpoint_supported() {
        return Err(
            Error::Ds("Recall physical checkpoint is unsupported by this engine".into()).into(),
        );
    }
    // This advertises a protocol, not readiness or successful reclamation.
    Ok(response(ns, db, false))
}

async fn checkpoint_inner(
    kvs: &Datastore,
    session: &Session,
    params: Array,
    remote: bool,
) -> Result<Value, RpcError> {
    let (ns, db) = authorize(kvs, session, Method::RecallPhysicalCheckpoint, remote)?;
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
    // Never run through kvs.process/execute: an ordinary transaction would hold
    // the very engine admission lease this administrative operation must drain.
    kvs.recall_physical_checkpoint(timeout_ms).await?;
    Ok(response(ns, db, true))
}

/// Advertise the exact supported protocol to an authenticated remote owner.
pub fn capabilities(kvs: &Datastore, session: &Session, params: Array) -> Result<Value, RpcError> {
    capabilities_inner(kvs, session, params, true)
}

/// Execute for an authenticated remote owner in the original selected scope.
pub async fn checkpoint(
    kvs: &Datastore,
    session: &Session,
    params: Array,
) -> Result<Value, RpcError> {
    checkpoint_inner(kvs, session, params, true).await
}

/// The in-process SDK retains trusted anonymous access when authentication is disabled.
pub fn local_capabilities(
    kvs: &Datastore,
    session: &Session,
    params: Array,
) -> Result<Value, RpcError> {
    capabilities_inner(kvs, session, params, false)
}

/// In-process counterpart; engine ownership and completion semantics are identical.
pub async fn local_checkpoint(
    kvs: &Datastore,
    session: &Session,
    params: Array,
) -> Result<Value, RpcError> {
    checkpoint_inner(kvs, session, params, false).await
}
