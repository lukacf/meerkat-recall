use crate::api::conn::{Command, DbResponse};
use crate::api::err::Error;
use crate::api::method::BoxFuture;
use crate::api::{Connection, Result};
use crate::method::OnceLockExt;
use crate::Surreal;
use std::borrow::Cow;
use std::future::IntoFuture;
use std::time::Duration;
use surrealdb_core::kvs::RecallHistorySnapshot;
use surrealdb_core::rpc::recall_history_inventory::PROTOCOL;
use surrealdb_core::rpc::recall_physical_checkpoint::MAX_TIMEOUT_MS;
use surrealdb_core::sql::Value;
#[cfg(not(target_family = "wasm"))]
use tokio::time::{timeout_at, Instant};
#[cfg(target_family = "wasm")]
use wasmtimer::{std::Instant, tokio::timeout_at};

#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct RecallHistoryInventory<'r, C: Connection> {
    pub(super) client: Cow<'r, Surreal<C>>,
    pub(super) namespace: String,
    pub(super) database: String,
    pub(super) timeout_ms: u64,
}

impl<C: Connection> RecallHistoryInventory<'_, C> {
    pub fn into_owned(self) -> RecallHistoryInventory<'static, C> {
        RecallHistoryInventory {
            client: Cow::Owned(self.client.into_owned()),
            namespace: self.namespace,
            database: self.database,
            timeout_ms: self.timeout_ms,
        }
    }
}

fn invalid() -> crate::Error {
    Error::InternalError("Invalid or incomplete Recall history inventory response".into()).into()
}

fn decode(value: Value, namespace: &str, database: &str) -> Result<RecallHistorySnapshot> {
    surrealdb_core::rpc::recall_physical_history_checkpoint::native_size(&value)?;
    let Value::Object(mut fields) = value else {
        return Err(invalid());
    };
    if fields.len() != 5
        || !matches!(fields.remove("protocol"), Some(Value::Strand(value)) if value.0 == PROTOCOL)
        || !matches!(fields.remove("namespace"), Some(Value::Strand(value)) if value.0 == namespace)
        || !matches!(fields.remove("database"), Some(Value::Strand(value)) if value.0 == database)
        || fields.remove("complete") != Some(Value::Bool(true))
    {
        return Err(invalid());
    }
    let snapshot =
        RecallHistorySnapshot::from_rpc_value(fields.remove("snapshot").ok_or_else(invalid)?)?;
    if !fields.is_empty() || snapshot.namespace != namespace || snapshot.database != database {
        return Err(invalid());
    }
    snapshot.verify()?;
    Ok(snapshot)
}

impl<'r, C: Connection> IntoFuture for RecallHistoryInventory<'r, C> {
    type Output = Result<RecallHistorySnapshot>;
    type IntoFuture = BoxFuture<'r, Self::Output>;
    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            if self.namespace.is_empty()
                || self.database.is_empty()
                || !(1..=MAX_TIMEOUT_MS).contains(&self.timeout_ms)
            {
                return Err(Error::InternalError(
                    "Recall history inventory requires scope and timeout 1..=300000 milliseconds"
                        .into(),
                )
                .into());
            }
            let deadline = Instant::now() + Duration::from_millis(self.timeout_ms);
            let timed_out = || {
                Error::InternalError(
                    "Recall history inventory deadline elapsed; no complete inventory available"
                        .into(),
                )
            };
            let router = self.client.inner.router.extract()?;
            let remaining_ms = u64::try_from(
                deadline
                    .saturating_duration_since(Instant::now())
                    .as_millis(),
            )
            .map_err(|_| timed_out())?;
            if remaining_ms == 0 {
                return Err(timed_out().into());
            }
            let value = timeout_at(deadline, async {
                let receiver = router
                    .send(Command::RecallHistoryInventory {
                        namespace: self.namespace.clone(),
                        database: self.database.clone(),
                        timeout_ms: remaining_ms,
                    })
                    .await?;
                match receiver.recv().await? {
                    Ok(DbResponse::Other(value)) => Ok(value),
                    Ok(DbResponse::Query(_)) => Err(invalid()),
                    Err(error) => Err(error),
                }
            })
            .await
            .map_err(|_| timed_out())??;
            let snapshot = decode(value, &self.namespace, &self.database)?;
            if Instant::now() >= deadline {
                return Err(timed_out().into());
            }
            Ok(snapshot)
        })
    }
}

#[cfg(all(test, not(target_family = "wasm")))]
mod tests {
    use super::super::recall_physical_history_checkpoint::tests::client;
    use super::*;
    use surrealdb_core::sql::Object;

    fn response() -> Value {
        let snapshot = RecallHistorySnapshot::new("ns".into(), "db".into(), Vec::new()).unwrap();
        let mut fields = Object::default();
        fields.insert(
            "protocol".into(),
            "meerkat-recall-source-history-inventory/v1".into(),
        );
        fields.insert("namespace".into(), "ns".into());
        fields.insert("database".into(), "db".into());
        fields.insert("complete".into(), true.into());
        fields.insert("snapshot".into(), snapshot.to_rpc_value());
        fields.into()
    }

    #[test]
    fn inventory_decode_refuses_partial_wrong_scope_and_corrupt_fingerprint() {
        decode(response(), "ns", "db").unwrap().verify().unwrap();
        assert!(decode(response(), "wrong", "db").is_err());
        for value in [Value::None, Value::Null, true.into()] {
            assert!(decode(value, "ns", "db").is_err());
        }
        let Value::Object(mut fields) = response() else {
            panic!("response");
        };
        fields.insert("complete".into(), false.into());
        assert!(decode(fields.into(), "ns", "db").is_err());
        let Value::Object(mut fields) = response() else {
            panic!("response");
        };
        let Some(Value::Object(snapshot)) = fields.get_mut("snapshot") else {
            panic!("snapshot");
        };
        snapshot.insert("fingerprint".into(), "0".repeat(64).into());
        assert!(decode(fields.into(), "ns", "db").is_err());
    }

    #[tokio::test]
    async fn inventory_sdk_is_one_owned_call_and_refuses_late_or_missing_completion() {
        for late in [false, true] {
            let (db, receiver) = client();
            let (result, ()) = tokio::join!(
                db.recall_source_history_inventory("ns", "db", if late { 50 } else { 60_000 })
                    .into_owned()
                    .into_future(),
                async {
                    let route = receiver.recv().await.unwrap();
                    assert!(matches!(
                        route.request.command,
                        Command::RecallHistoryInventory { .. }
                    ));
                    if late {
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                    let _ = route
                        .response
                        .send(Ok(DbResponse::Other(if late {
                            response()
                        } else {
                            Value::None
                        })))
                        .await;
                }
            );
            assert!(result.is_err());
            assert!(receiver.is_empty());
        }
    }
}
