use crate::api::conn::Command;
use crate::api::err::Error;
use crate::api::method::BoxFuture;
use crate::api::{Connection, Result};
use crate::method::OnceLockExt;
use crate::Surreal;
use std::borrow::Cow;
use std::future::IntoFuture;
use std::time::Duration;
use surrealdb_core::rpc::recall_physical_checkpoint::{MAX_TIMEOUT_MS, PROTOCOL};
use surrealdb_core::sql::{Number, Value};
#[cfg(not(target_family = "wasm"))]
use tokio::time::{timeout_at, Instant};
#[cfg(target_family = "wasm")]
use wasmtimer::{std::Instant, tokio::timeout_at};

/// A version-checked administrative physical checkpoint future.
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct RecallPhysicalCheckpoint<'r, C: Connection> {
    pub(super) client: Cow<'r, Surreal<C>>,
    pub(super) timeout_ms: u64,
}

impl<C: Connection> RecallPhysicalCheckpoint<'_, C> {
    /// Converts to an owned future which can be moved to a different thread.
    pub fn into_owned(self) -> RecallPhysicalCheckpoint<'static, C> {
        RecallPhysicalCheckpoint {
            client: Cow::Owned(self.client.into_owned()),
            timeout_ms: self.timeout_ms,
        }
    }
}

fn invalid_response() -> crate::Error {
    Error::InternalError("Invalid Recall physical checkpoint protocol response".into()).into()
}

fn timed_out() -> crate::Error {
    Error::InternalError(
        "Recall physical checkpoint deadline elapsed; completion is uncertain".into(),
    )
    .into()
}

// Native values are checked without a JSON conversion or permissive defaults.
fn response_scope(value: Value, complete: bool) -> Result<(String, String)> {
    let Value::Object(mut fields) = value else {
        return Err(invalid_response());
    };
    if fields.len() != if complete { 5 } else { 4 }
        || !matches!(fields.remove("protocol"), Some(Value::Strand(value)) if value.0 == PROTOCOL)
        || !matches!(
            fields.remove("max_timeout_ms"),
            Some(Value::Number(Number::Int(300_000)))
        )
        || (complete && fields.remove("complete") != Some(Value::Bool(true)))
    {
        return Err(invalid_response());
    }
    let Some(Value::Strand(namespace)) = fields.remove("namespace") else {
        return Err(invalid_response());
    };
    let Some(Value::Strand(database)) = fields.remove("database") else {
        return Err(invalid_response());
    };
    if namespace.0.is_empty() || database.0.is_empty() || !fields.is_empty() {
        return Err(invalid_response());
    }
    Ok((namespace.0, database.0))
}

impl<'r, C: Connection> IntoFuture for RecallPhysicalCheckpoint<'r, C> {
    type Output = Result<()>;
    type IntoFuture = BoxFuture<'r, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            if !(1..=MAX_TIMEOUT_MS).contains(&self.timeout_ms) {
                return Err(Error::InternalError(
                    "Recall physical checkpoint timeout must be 1..=300000 milliseconds".into(),
                )
                .into());
            }
            // One original budget includes capability negotiation, dispatch and
            // receipt validation. Never renew the full budget after the handshake.
            let deadline = Instant::now() + Duration::from_millis(self.timeout_ms);
            let router = self.client.inner.router.extract()?;
            let capability = timeout_at(
                deadline,
                router.execute_value(Command::RecallPhysicalCheckpointCapabilities),
            )
            .await
            .map_err(|_| timed_out())??;
            let scope = response_scope(capability.into_inner(), false)?;
            let remaining_ms = u64::try_from(
                deadline
                    .saturating_duration_since(Instant::now())
                    .as_millis(),
            )
            .map_err(|_| timed_out())?;
            if remaining_ms == 0 {
                return Err(timed_out());
            }
            // A timed-out wait does not cancel server-owned checkpoint work. The
            // caller must keep erasure/restore admission closed on this error.
            let result = timeout_at(
                deadline,
                router.execute_value(Command::RecallPhysicalCheckpoint {
                    namespace: scope.0.clone(),
                    database: scope.1.clone(),
                    timeout_ms: remaining_ms,
                }),
            )
            .await
            .map_err(|_| timed_out())??;
            if response_scope(result.into_inner(), true)? != scope {
                return Err(invalid_response());
            }
            if Instant::now() >= deadline {
                return Err(timed_out());
            }
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use surrealdb_core::sql::Object;

    fn response(complete: bool) -> Value {
        let mut fields = Object::default();
        fields.insert("protocol".into(), PROTOCOL.into());
        fields.insert("namespace".into(), "scope-ns".into());
        fields.insert("database".into(), "scope-db".into());
        fields.insert("max_timeout_ms".into(), 300_000_i64.into());
        if complete {
            fields.insert("complete".into(), true.into());
        }
        Value::Object(fields)
    }

    #[test]
    fn checkpoint_response_requires_exact_native_completion_and_scope() {
        assert_eq!(
            response_scope(response(false), false).unwrap(),
            ("scope-ns".into(), "scope-db".into())
        );
        assert_eq!(
            response_scope(response(true), true).unwrap(),
            ("scope-ns".into(), "scope-db".into())
        );
        for value in [Value::None, Value::Null, true.into(), response(false)] {
            assert!(response_scope(value, true).is_err());
        }
        for (field, replacement) in [
            ("protocol", "meerkat-recall-physical-checkpoint/v2".into()),
            ("namespace", "".into()),
            ("database", Value::None),
            ("max_timeout_ms", 300_000.0_f64.into()),
            ("complete", false.into()),
            ("complete", "true".into()),
            ("unexpected", true.into()),
        ] {
            let Value::Object(mut fields) = response(true) else {
                unreachable!()
            };
            fields.insert(field.into(), replacement);
            assert!(
                response_scope(Value::Object(fields), true).is_err(),
                "{field}"
            );
        }
    }


    #[cfg(not(target_family = "wasm"))]
    fn client() -> (
        Surreal<crate::engine::any::Any>,
        async_channel::Receiver<crate::api::conn::Route>,
    ) {
        use crate::api::conn::Router;
        use std::collections::HashSet;
        use std::sync::{atomic::AtomicI64, OnceLock};
        let (sender, receiver) = async_channel::bounded(1);
        let router = Router {
            sender,
            config: Default::default(),
            last_id: AtomicI64::new(0),
            features: HashSet::new(),
        };
        (
            (
                OnceLock::with_value(router),
                tokio::sync::watch::channel(None),
            )
                .into(),
            receiver,
        )
    }

    #[cfg(not(target_family = "wasm"))]
    #[tokio::test]
    async fn checkpoint_sdk_refuses_missing_capability_and_uncertain_completion() {
        use crate::api::conn::DbResponse;
        // An upstream/unpatched server's method error must prevent dispatch.
        let (db, receiver) = client();
        let (result, ()) =
            tokio::join!(db.recall_physical_checkpoint(60_000).into_future(), async {
                let route = receiver.recv().await.unwrap();
                assert!(matches!(
                    route.request.command,
                    Command::RecallPhysicalCheckpointCapabilities
                ));
                route
                    .response
                    .send(Err(Error::InternalError("Method not found".into()).into()))
                    .await
                    .unwrap();
            });
        assert!(result.is_err());
        assert!(receiver.is_empty());

        let mut wrong_scope = response(true);
        if let Value::Object(fields) = &mut wrong_scope {
            fields.insert("database".into(), "another-db".into());
        }
        for completion in [Value::None, Value::Null, response(false), wrong_scope] {
            let (db, receiver) = client();
            let (result, ()) =
                tokio::join!(db.recall_physical_checkpoint(60_000).into_future(), async {
                    let capability = receiver.recv().await.unwrap();
                    capability
                        .response
                        .send(Ok(DbResponse::Other(response(false))))
                        .await
                        .unwrap();
                    let execute = receiver.recv().await.unwrap();
                    assert!(matches!(
                        execute.request.command,
                        Command::RecallPhysicalCheckpoint { .. }
                    ));
                    execute
                        .response
                        .send(Ok(DbResponse::Other(completion)))
                        .await
                        .unwrap();
                });
            assert!(result.is_err());
            assert!(receiver.is_empty());
        }
    }

    #[cfg(not(target_family = "wasm"))]
    #[tokio::test]
    async fn checkpoint_sdk_deducts_handshake_time_from_original_budget() {
        use crate::api::conn::DbResponse;
        let (db, receiver) = client();
        let (result, ()) =
            tokio::join!(db.recall_physical_checkpoint(5_000).into_future(), async {
                let capability = receiver.recv().await.unwrap();
                tokio::time::sleep(Duration::from_millis(20)).await;
                capability
                    .response
                    .send(Ok(DbResponse::Other(response(false))))
                    .await
                    .unwrap();
                let execute = receiver.recv().await.unwrap();
                let Command::RecallPhysicalCheckpoint {
                    namespace,
                    database,
                    timeout_ms,
                } = execute.request.command
                else {
                    panic!("expected administrative command")
                };
                assert_eq!(
                    (namespace.as_str(), database.as_str()),
                    ("scope-ns", "scope-db")
                );
                assert!((1..=4_980).contains(&timeout_ms));
                execute
                    .response
                    .send(Ok(DbResponse::Other(response(true))))
                    .await
                    .unwrap();
            });
        result.unwrap();
    }

    #[cfg(not(target_family = "wasm"))]
    #[tokio::test]
    async fn checkpoint_sdk_deadline_never_turns_late_server_completion_into_success() {
        use crate::api::conn::DbResponse;
        for expire_during_capability in [true, false] {
            let (db, receiver) = client();
            let (result, ()) =
                tokio::join!(db.recall_physical_checkpoint(100).into_future(), async {
                    let capability = receiver.recv().await.unwrap();
                    if expire_during_capability {
                        tokio::time::sleep(Duration::from_millis(150)).await;
                        // Late response may fail because the waiter is gone.
                        let _ = capability
                            .response
                            .send(Ok(DbResponse::Other(response(false))))
                            .await;
                    } else {
                        capability
                            .response
                            .send(Ok(DbResponse::Other(response(false))))
                            .await
                            .unwrap();
                        let execute = receiver.recv().await.unwrap();
                        assert!(matches!(
                            execute.request.command,
                            Command::RecallPhysicalCheckpoint { .. }
                        ));
                        // The request remains owned by the server; client timeout
                        // cannot manufacture either cancellation or completion.
                        tokio::time::sleep(Duration::from_millis(150)).await;
                        let _ = execute
                            .response
                            .send(Ok(DbResponse::Other(response(true))))
                            .await;
                    }
                });
            assert!(result
                .unwrap_err()
                .to_string()
                .contains("completion is uncertain"));
            assert!(receiver.is_empty());
        }
    }
}
