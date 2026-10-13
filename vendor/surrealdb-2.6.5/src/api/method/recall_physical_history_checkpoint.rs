use crate::api::conn::{Command, DbResponse};
use crate::api::err::Error;
use crate::api::method::BoxFuture;
use crate::api::{Connection, Result};
use crate::method::OnceLockExt;
use crate::Surreal;
use std::borrow::Cow;
use std::future::IntoFuture;
use std::time::Duration;
use surrealdb_core::kvs::RecallHistoryAuthority;
use surrealdb_core::rpc::recall_physical_checkpoint::MAX_TIMEOUT_MS;
use surrealdb_core::rpc::recall_physical_history_checkpoint::{validate_authorities, PROTOCOL};
use surrealdb_core::sql::{Number, Value};
#[cfg(not(target_family = "wasm"))]
use tokio::time::{timeout_at, Instant};
#[cfg(target_family = "wasm")]
use wasmtimer::{std::Instant, tokio::timeout_at};

/// A physical source-history checkpoint under the original journal authorities.
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct RecallPhysicalHistoryCheckpoint<'r, C: Connection> {
    pub(super) client: Cow<'r, Surreal<C>>,
    pub(super) namespace: String,
    pub(super) database: String,
    pub(super) timeout_ms: u64,
    pub(super) authorities: Vec<RecallHistoryAuthority>,
}

impl<C: Connection> std::fmt::Debug for RecallPhysicalHistoryCheckpoint<'_, C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RecallPhysicalHistoryCheckpoint")
            .field("namespace", &self.namespace)
            .field("database", &self.database)
            .field("timeout_ms", &self.timeout_ms)
            .field("authority_count", &self.authorities.len())
            .finish_non_exhaustive()
    }
}

impl<C: Connection> RecallPhysicalHistoryCheckpoint<'_, C> {
    /// Retain the exact authority vector in an owned future.
    pub fn into_owned(self) -> RecallPhysicalHistoryCheckpoint<'static, C> {
        RecallPhysicalHistoryCheckpoint {
            client: Cow::Owned(self.client.into_owned()),
            namespace: self.namespace,
            database: self.database,
            timeout_ms: self.timeout_ms,
            authorities: self.authorities,
        }
    }
}

fn invalid() -> crate::Error {
    Error::InternalError("Invalid Recall physical history checkpoint response".into()).into()
}
fn timed_out() -> crate::Error {
    Error::InternalError(
        "Recall physical history checkpoint deadline elapsed; completion is uncertain".into(),
    )
    .into()
}

fn check_response(value: Value, namespace: &str, database: &str, count: usize) -> Result<()> {
    let Value::Object(mut fields) = value else {
        return Err(invalid());
    };
    if fields.len() != 6
        || !matches!(fields.remove("protocol"), Some(Value::Strand(value)) if value.0 == PROTOCOL)
        || !matches!(fields.remove("namespace"), Some(Value::Strand(value)) if value.0 == namespace)
        || !matches!(fields.remove("database"), Some(Value::Strand(value)) if value.0 == database)
        || !matches!(
            fields.remove("max_timeout_ms"),
            Some(Value::Number(Number::Int(300_000)))
        )
        || !matches!(fields.remove("authority_count"), Some(Value::Number(Number::Int(value))) if usize::try_from(value).ok() == Some(count))
        || fields.remove("complete") != Some(Value::Bool(true))
        || !fields.is_empty()
    {
        return Err(invalid());
    }
    Ok(())
}

impl<'r, C: Connection> IntoFuture for RecallPhysicalHistoryCheckpoint<'r, C> {
    type Output = Result<()>;
    type IntoFuture = BoxFuture<'r, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            if !(1..=MAX_TIMEOUT_MS).contains(&self.timeout_ms) {
                return Err(Error::InternalError(
                    "Recall physical history checkpoint timeout must be 1..=300000 milliseconds"
                        .into(),
                )
                .into());
            }
            let deadline = Instant::now() + Duration::from_millis(self.timeout_ms);
            validate_authorities(&self.namespace, &self.database, &self.authorities)?;
            let router = self.client.inner.router.extract()?;
            let count = self.authorities.len();
            let remaining_ms = u64::try_from(
                deadline
                    .saturating_duration_since(Instant::now())
                    .as_millis(),
            )
            .map_err(|_| timed_out())?;
            if remaining_ms == 0 {
                return Err(timed_out());
            }
            // A single versioned call; unsupported servers cannot fall back to an
            // unscoped checkpoint. Ambiguous completion stays an error.
            let result = timeout_at(deadline, async {
                let receiver = router
                    .send(Command::RecallPhysicalHistoryCheckpoint {
                        namespace: self.namespace.clone(),
                        database: self.database.clone(),
                        timeout_ms: remaining_ms,
                        authorities: self.authorities,
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
            check_response(result, &self.namespace, &self.database, count)?;
            if Instant::now() >= deadline {
                return Err(timed_out());
            }
            Ok(())
        })
    }
}

#[cfg(all(test, not(target_family = "wasm")))]
pub(super) mod tests {
    use super::*;
    use std::collections::{BTreeSet, HashSet};
    use std::sync::{atomic::AtomicI64, OnceLock};
    use surrealdb_core::sql::Object;

    pub(crate) fn client() -> (
        Surreal<crate::engine::any::Any>,
        async_channel::Receiver<crate::api::conn::Route>,
    ) {
        let (sender, receiver) = async_channel::bounded(1);
        let router = crate::api::conn::Router {
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

    fn authority() -> RecallHistoryAuthority {
        RecallHistoryAuthority::new(
            "ns".into(),
            "db".into(),
            "11111111-1111-4111-8111-111111111111".into(),
            "space".into(),
            [7; 32],
            vec!["entity".into()],
            BTreeSet::from(["a".repeat(64)]),
        )
        .unwrap()
        .bind_history("b".repeat(64), BTreeSet::new(), BTreeSet::new())
        .unwrap()
    }

    fn response() -> Value {
        let mut fields = Object::default();
        fields.insert(
            "protocol".into(),
            "meerkat-recall-physical-history-checkpoint/v1".into(),
        );
        fields.insert("namespace".into(), "ns".into());
        fields.insert("database".into(), "db".into());
        fields.insert("max_timeout_ms".into(), 300_000_i64.into());
        fields.insert("authority_count".into(), 1_i64.into());
        fields.insert("complete".into(), true.into());
        fields.into()
    }

    #[test]
    fn history_checkpoint_response_requires_exact_scope_count_protocol_and_complete() {
        check_response(response(), "ns", "db", 1).unwrap();
        for value in [Value::None, Value::Null, Value::Bool(true)] {
            assert!(check_response(value, "ns", "db", 1).is_err());
        }
        assert!(check_response(response(), "ns", "other", 1).is_err());
        assert!(check_response(response(), "ns", "db", 2).is_err());
        for (key, value) in [
            ("protocol", Value::from("v2")),
            ("complete", false.into()),
            ("authority_count", 1.0_f64.into()),
            ("max_timeout_ms", "300000".into()),
            ("extra", true.into()),
        ] {
            let Value::Object(mut fields) = response() else {
                panic!("response");
            };
            fields.insert(key.into(), value);
            assert!(check_response(fields.into(), "ns", "db", 1).is_err());
        }
    }

    #[tokio::test]
    async fn history_checkpoint_sdk_transmits_original_authorities_and_never_retries_invalid_reply()
    {
        for reply in [
            Ok(DbResponse::Other(response())),
            Ok(DbResponse::Other(Value::None)),
            Err(Error::InternalError("Method not found".into()).into()),
            Ok(DbResponse::Query(crate::api::method::query::Response::new())),
        ] {
            let valid = matches!(&reply, Ok(DbResponse::Other(Value::Object(_))));
            let (db, receiver) = client();
            let (result, ()) = tokio::join!(
                db.recall_physical_checkpoint_with_history("ns", "db", 60_000, vec![authority()])
                    .into_owned()
                    .into_future(),
                async {
                    let route = receiver.recv().await.unwrap();
                    let Command::RecallPhysicalHistoryCheckpoint {
                        namespace,
                        database,
                        timeout_ms,
                        authorities,
                    } = route.request.command
                    else {
                        panic!("one physical command required");
                    };
                    assert_eq!((namespace.as_str(), database.as_str()), ("ns", "db"));
                    assert!((1..=60_000).contains(&timeout_ms));
                    assert_eq!(authorities.len(), 1);
                    assert_eq!(authorities[0].to_rpc_value(), authority().to_rpc_value());
                    route.response.send(reply).await.unwrap();
                }
            );
            assert_eq!(result.is_ok(), valid);
            assert!(receiver.is_empty());
        }
    }

    #[tokio::test]
    async fn history_checkpoint_sdk_keeps_original_deadline_after_dispatch() {
        let (db, receiver) = client();
        let (result, ()) = tokio::join!(
            db.recall_physical_checkpoint_with_history("ns", "db", 50, vec![authority()])
                .into_future(),
            async {
                let route = receiver.recv().await.unwrap();
                tokio::time::sleep(Duration::from_millis(100)).await;
                let _ = route.response.send(Ok(DbResponse::Other(response()))).await;
            }
        );
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("completion is uncertain"));
        assert!(receiver.is_empty());
    }
}
