//! Actual native Mem dispatch/authentication controls. Physical RocksDB file
//! reclamation is covered separately by the engine owner's disk tests.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashSet;
use std::sync::{Arc, RwLock};
use surrealdb::dbs::capabilities::{Capabilities, MethodTarget, Targets};
use surrealdb::dbs::Session;
use surrealdb::engine::local::Mem;
use surrealdb::iam::{Auth, Level, Role};
use surrealdb::kvs::Datastore;
use surrealdb::rpc::{Data, Method, RpcContext, RpcError, RpcProtocolV1, RpcProtocolV2};
use surrealdb::sql::{Array, Object, Value};
use surrealdb::Surreal;
use tokio::sync::Semaphore;

// Independent wire literal: changing the implementation protocol must not silently
// change both sides of these controls.
const PROTOCOL: &str = "meerkat-recall-physical-checkpoint/v1";

struct Context {
    kvs: Datastore,
    session: RwLock<Arc<Session>>,
    lock: Arc<Semaphore>,
}

impl RpcContext for Context {
    fn kvs(&self) -> &Datastore {
        &self.kvs
    }
    fn lock(&self) -> Arc<Semaphore> {
        self.lock.clone()
    }
    fn session(&self) -> Arc<Session> {
        self.session.read().expect("session lock").clone()
    }
    fn set_session(&self, session: Arc<Session>) {
        *self.session.write().expect("session lock") = session;
    }
    fn version_data(&self) -> Data {
        Value::from("native-test").into()
    }
}

impl RpcProtocolV1 for Context {}
impl RpcProtocolV2 for Context {}

async fn context(session: Session, capabilities: Capabilities) -> Context {
    Context {
        kvs: Datastore::new("memory")
            .await
            .expect("native memory datastore")
            .with_auth_enabled(true)
            .with_capabilities(capabilities),
        session: RwLock::new(Arc::new(session)),
        lock: Arc::new(Semaphore::new(1)),
    }
}

fn owner() -> Session {
    Session::owner()
        .with_ns("checkpoint-ns")
        .with_db("checkpoint-db")
}

fn capabilities() -> Array {
    vec![Value::from(PROTOCOL)].into()
}

fn checkpoint(timeout: Value) -> Array {
    vec![
        Value::from(PROTOCOL),
        Value::from("checkpoint-ns"),
        Value::from("checkpoint-db"),
        timeout,
    ]
    .into()
}

async fn invoke(
    ctx: &Context,
    version: u8,
    method: &str,
    params: Array,
) -> Result<Value, RpcError> {
    RpcContext::execute(
        ctx,
        Some(version),
        Method::parse_case_sensitive(method),
        params,
    )
    .await
    .and_then(|data| Value::try_from(data).map_err(Into::into))
}

fn expected(complete: bool) -> Value {
    let mut fields = Object::default();
    fields.insert("protocol".into(), PROTOCOL.into());
    fields.insert("namespace".into(), "checkpoint-ns".into());
    fields.insert("database".into(), "checkpoint-db".into());
    fields.insert("max_timeout_ms".into(), 300_000_i64.into());
    if complete {
        fields.insert("complete".into(), true.into());
    }
    Value::Object(fields)
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
async fn native_physical_rpc_v1_v2_complete_only_after_engine_and_keep_survivor() {
    let ctx = context(owner(), Capabilities::default()).await;
    let mut created = ctx
        .kvs
        .execute("CREATE survivor:one SET payload = 'keep';", &owner(), None)
        .await
        .expect("create independent survivor");
    created.remove(0).result.expect("survivor committed");
    for version in [1, 2] {
        assert_eq!(
            invoke(
                &ctx,
                version,
                "recall_physical_checkpoint_capabilities",
                capabilities()
            )
            .await
            .expect("versioned capability"),
            expected(false)
        );
        assert_eq!(
            invoke(
                &ctx,
                version,
                "recall_physical_checkpoint",
                checkpoint(60_000_i64.into())
            )
            .await
            .expect("native memory checkpoint"),
            expected(true)
        );
    }
    let mut selected = ctx
        .kvs
        .execute("RETURN survivor:one.payload;", &owner(), None)
        .await
        .expect("read survivor");
    assert_eq!(
        selected.remove(0).result.expect("survivor query"),
        Value::from("keep")
    );
}

#[tokio::test]
async fn native_physical_rpc_denies_anonymous_record_database_namespace_and_non_owner() {
    let mut record = owner();
    record.au = Arc::new(Auth::for_record(
        "person:one".into(),
        "checkpoint-ns",
        "checkpoint-db",
        "users",
    ));
    let sessions = [
        Session::default()
            .with_ns("checkpoint-ns")
            .with_db("checkpoint-db"),
        Session::for_level(
            Level::Database("checkpoint-ns".into(), "checkpoint-db".into()),
            Role::Owner,
        )
        .with_ns("checkpoint-ns")
        .with_db("checkpoint-db"),
        Session::for_level(Level::Namespace("checkpoint-ns".into()), Role::Owner)
            .with_ns("checkpoint-ns")
            .with_db("checkpoint-db"),
        Session::editor()
            .with_ns("checkpoint-ns")
            .with_db("checkpoint-db"),
        Session::viewer()
            .with_ns("checkpoint-ns")
            .with_db("checkpoint-db"),
        record,
    ];
    for session in sessions {
        // Even an auth-disabled remote server must not expose administrative
        // compaction to an anonymous caller.
        let mut ctx = context(session, Capabilities::default()).await;
        ctx.kvs = ctx.kvs.with_auth_enabled(false);
        for version in [1, 2] {
            assert!(matches!(
                invoke(
                    &ctx,
                    version,
                    "recall_physical_checkpoint_capabilities",
                    capabilities()
                )
                .await,
                Err(RpcError::MethodNotAllowed)
            ));
            assert!(matches!(
                invoke(
                    &ctx,
                    version,
                    "recall_physical_checkpoint",
                    checkpoint(60_000_i64.into())
                )
                .await,
                Err(RpcError::MethodNotAllowed)
            ));
        }
    }
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
async fn native_physical_rpc_rejects_expired_missing_changed_and_empty_scope() {
    let ctx = context(owner(), Capabilities::default()).await;
    invoke(
        &ctx,
        1,
        "recall_physical_checkpoint_capabilities",
        capabilities(),
    )
    .await
    .expect("initial scope");
    let mut expired = owner();
    expired.exp = Some(1);
    let sessions = [
        expired,
        Session::owner(),
        Session::owner().with_ns("checkpoint-ns"),
        owner().with_ns("another-ns"),
        owner().with_db("another-db"),
        owner().with_db(""),
    ];
    for session in sessions {
        ctx.set_session(Arc::new(session));
        for version in [1, 2] {
            assert!(invoke(
                &ctx,
                version,
                "recall_physical_checkpoint",
                checkpoint(60_000_i64.into())
            )
            .await
            .is_err());
        }
    }
    ctx.set_session(Arc::new(owner()));
    assert_eq!(
        invoke(
            &ctx,
            2,
            "recall_physical_checkpoint",
            checkpoint(1_i64.into())
        )
        .await
        .expect("restored original scope"),
        expected(true)
    );
}

#[tokio::test]
async fn native_physical_rpc_rejects_protocol_type_arity_and_timeout_without_fallback() {
    let ctx = context(owner(), Capabilities::default()).await;
    let mut wrong_version = checkpoint(1_i64.into());
    wrong_version[0] = "meerkat-recall-physical-checkpoint/v2".into();
    let mut extra = checkpoint(1_i64.into());
    extra.push("/not-an-authorized-path".into());
    let cases = vec![
        Array::default(),
        capabilities(),
        wrong_version,
        extra,
        checkpoint(0_i64.into()),
        checkpoint((-1_i64).into()),
        checkpoint(300_001_i64.into()),
        checkpoint(1.0_f64.into()),
        checkpoint("1000".into()),
        checkpoint(Value::None),
    ];
    for version in [1, 2] {
        for params in &cases {
            assert!(matches!(
                invoke(&ctx, version, "recall_physical_checkpoint", params.clone()).await,
                Err(RpcError::InvalidParams)
            ));
        }
        for params in [
            Array::default(),
            vec![Value::from("v2")].into(),
            vec![Value::from(PROTOCOL), Value::None].into(),
        ] {
            assert!(matches!(
                invoke(
                    &ctx,
                    version,
                    "recall_physical_checkpoint_capabilities",
                    params
                )
                .await,
                Err(RpcError::InvalidParams)
            ));
        }
        assert!(matches!(
            invoke(
                &ctx,
                version,
                "recall_physical_checkpoint_v2",
                capabilities()
            )
            .await,
            Err(RpcError::MethodNotFound)
        ));
        assert_eq!(
            invoke(
                &ctx,
                version,
                "recall_physical_checkpoint",
                checkpoint(300_000_i64.into())
            )
            .await
            .expect("inclusive upper bound"),
            expected(true)
        );
    }
}

#[tokio::test]
async fn native_physical_rpc_obeys_each_existing_method_capability() {
    for method in [
        "recall_physical_checkpoint_capabilities",
        "recall_physical_checkpoint",
    ] {
        let denied = MethodTarget {
            method: Method::parse_case_sensitive(method),
        };
        let caps =
            Capabilities::default().without_rpc_methods(Targets::Some(HashSet::from([denied])));
        let ctx = context(owner(), caps).await;
        let params = if method.ends_with("capabilities") {
            capabilities()
        } else {
            checkpoint(1_i64.into())
        };
        for version in [1, 2] {
            assert!(matches!(
                invoke(&ctx, version, method, params.clone()).await,
                Err(RpcError::MethodNotAllowed)
            ));
        }
    }
}

#[tokio::test]
async fn native_physical_sdk_uses_same_scope_protocol_and_refuses_invalid_bounds() {
    let db = Surreal::new::<Mem>(()).await.expect("actual local SDK");
    assert!(db.recall_physical_checkpoint(60_000).await.is_err());
    db.use_ns("checkpoint-ns")
        .use_db("checkpoint-db")
        .await
        .expect("selected scope");
    db.recall_physical_checkpoint(60_000)
        .into_owned()
        .await
        .expect("native SDK completion");
    for timeout in [0, 300_001, u64::MAX] {
        assert!(db.recall_physical_checkpoint(timeout).await.is_err());
    }
    db.query("CREATE retained:one SET value = 'unchanged';")
        .await
        .expect("retained write")
        .check()
        .expect("committed");
    db.recall_physical_checkpoint(60_000)
        .await
        .expect("second checkpoint");
    let mut response = db
        .query("RETURN retained:one.value;")
        .await
        .expect("read retained");
    assert_eq!(
        response.take::<Option<String>>(0).expect("typed survivor"),
        Some("unchanged".into())
    );
}
