//! Actual native Mem dispatch/authentication controls. Selective history and disk retirement
//! are covered separately by the source-authority and engine owners.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::{BTreeMap, HashSet};
use std::sync::{Arc, RwLock};
use surrealdb::dbs::capabilities::{Capabilities, MethodTarget, Targets};
use surrealdb::dbs::Session;
use surrealdb::engine::local::Mem;
use surrealdb::iam::{Auth, Level, Role};
use surrealdb::kvs::recall_history::NativeHistoryKey;
use surrealdb::kvs::Datastore;
use surrealdb::rpc::{Data, Method, RpcContext, RpcError, RpcProtocolV1, RpcProtocolV2};
use surrealdb::sql::{Array, Object, Value};
use surrealdb::Surreal;
use tokio::sync::Semaphore;

// Independent wire literal: changing the implementation protocol must not silently
// change both sides of these controls.
const PROTOCOL: &str = "meerkat-recall-physical-history-checkpoint/v1";

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

fn unbound_authority(space: &str) -> surrealdb::method::RecallHistoryAuthority {
    surrealdb::method::RecallHistoryAuthority::new(
        "checkpoint-ns".into(),
        "checkpoint-db".into(),
        "11111111-1111-4111-8111-111111111111".into(),
        space.into(),
        [7; 32],
        vec!["entity".into()],
        std::collections::BTreeSet::from(["a".repeat(64)]),
    )
    .expect("original journal authority fixture")
}

fn authority(space: &str) -> surrealdb::method::RecallHistoryAuthority {
    // Negative/wire controls need a syntactically bound authority, but never
    // assert that this synthetic fingerprint authorizes actual engine work.
    unbound_authority(space)
        .bind_history(
            "0".repeat(64),
            std::collections::BTreeSet::default(),
            std::collections::BTreeSet::default(),
        )
        .unwrap()
}

fn bound_authorities(
    snapshot: &surrealdb::method::RecallHistorySnapshot,
) -> Vec<surrealdb::method::RecallHistoryAuthority> {
    ["first", "second"]
        .into_iter()
        .map(|space| {
            unbound_authority(space)
                .bind_history(
                    snapshot.fingerprint.clone(),
                    std::collections::BTreeSet::default(),
                    std::collections::BTreeSet::default(),
                )
                .unwrap()
        })
        .collect()
}

async fn actual_checkpoint(ctx: &Context) -> Array {
    let snapshot = ctx
        .kvs
        .recall_history_inventory(60_000, "checkpoint-ns", "checkpoint-db")
        .await
        .unwrap();
    let mut request = checkpoint();
    request[4] = Value::Array(
        bound_authorities(&snapshot)
            .iter()
            .map(surrealdb::method::RecallHistoryAuthority::to_rpc_value)
            .collect::<Vec<_>>()
            .into(),
    );
    request
}

fn checkpoint() -> Array {
    vec![
        PROTOCOL.into(),
        "checkpoint-ns".into(),
        "checkpoint-db".into(),
        60_000_i64.into(),
        Value::Array(
            vec![
                authority("first").to_rpc_value(),
                authority("second").to_rpc_value(),
            ]
            .into(),
        ),
    ]
    .into()
}

async fn invoke(ctx: &Context, version: u8, params: Array) -> Result<Value, RpcError> {
    RpcContext::execute(
        ctx,
        Some(version),
        Method::parse_case_sensitive("recall_physical_history_checkpoint"),
        params,
    )
    .await
    .and_then(|data| Value::try_from(data).map_err(Into::into))
}

fn expected() -> Value {
    let mut fields = Object::default();
    fields.insert("protocol".into(), PROTOCOL.into());
    fields.insert("namespace".into(), "checkpoint-ns".into());
    fields.insert("database".into(), "checkpoint-db".into());
    fields.insert("max_timeout_ms".into(), 300_000_i64.into());
    fields.insert("authority_count".into(), 2_i64.into());
    fields.insert("complete".into(), true.into());
    fields.into()
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
async fn native_physical_history_rpc_v1_v2_retains_all_authorities_and_survivor() {
    let ctx = context(owner(), Capabilities::default()).await;
    // Use the actual source-less Recall entity writer shape, including its
    // complete native envelope. The history policy does not exempt arbitrary
    // unscoped data merely because this test wants an independent survivor.
    let entity = elephant_types::records::Entity::new(
        elephant_types::records::base::BaseFields::new(
            elephant_types::RecordId::new_v4(elephant_types::TableName::Entity),
            "named-independent".into(),
            "native-history-control".into(),
        ),
        elephant_types::records::entity::EntityType::new("project"),
        "Independent manual project".into(),
    );
    let identity = Value::Thing(crate::surreal::thing_from_record_id(&entity.base.id));
    let rows = ctx
        .kvs
        .execute(
            "CREATE $id CONTENT $body RETURN AFTER;",
            &owner(),
            Some(BTreeMap::from([
                ("id".into(), identity.clone()),
                (
                    "body".into(),
                    crate::surreal::entity_to_value(&entity).unwrap(),
                ),
            ])),
        )
        .await
        .unwrap();
    let original = rows[0].result.as_ref().unwrap().clone();
    assert!(matches!(&original, Value::Array(rows) if rows.len() == 1));
    for version in [1, 2] {
        assert_eq!(
            invoke(&ctx, version, actual_checkpoint(&ctx).await)
                .await
                .unwrap(),
            expected()
        );
    }
    let rows = ctx
        .kvs
        .execute(
            "SELECT * FROM $id;",
            &owner(),
            Some(BTreeMap::from([("id".into(), identity)])),
        )
        .await
        .unwrap();
    assert_eq!(rows[0].result.as_ref().unwrap(), &original);
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
async fn native_physical_history_rpc_refuses_unknown_unscoped_data() {
    for version in [1, 2] {
        let ctx = context(owner(), Capabilities::default()).await;
        let rows = ctx
            .kvs
            .execute(
                "CREATE unproven:one SET payload = 'unknown ownership';",
                &owner(),
                None,
            )
            .await
            .unwrap();
        assert!(rows[0].result.is_ok());
        assert!(invoke(&ctx, version, actual_checkpoint(&ctx).await)
            .await
            .is_err());
    }
}

#[tokio::test]
async fn native_physical_history_rpc_denies_non_owner_expired_missing_and_changed_scope() {
    let mut expired = owner();
    expired.exp = Some(1);
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
        Session::for_level(Level::Namespace("checkpoint-ns".into()), Role::Owner)
            .with_ns("checkpoint-ns")
            .with_db("checkpoint-db"),
        Session::for_level(
            Level::Database("checkpoint-ns".into(), "checkpoint-db".into()),
            Role::Owner,
        )
        .with_ns("checkpoint-ns")
        .with_db("checkpoint-db"),
        Session::editor()
            .with_ns("checkpoint-ns")
            .with_db("checkpoint-db"),
        record,
        expired,
        Session::owner(),
        owner().with_ns("another"),
        owner().with_db("another"),
        owner().with_db(""),
    ];
    for session in sessions {
        let mut ctx = context(session, Capabilities::default()).await;
        ctx.kvs = ctx.kvs.with_auth_enabled(false);
        for version in [1, 2] {
            assert!(invoke(&ctx, version, checkpoint()).await.is_err());
        }
    }
}

#[tokio::test]
#[expect(
    clippy::panic,
    reason = "Unexpected native values are explicit failures of this regression oracle."
)]
async fn native_physical_history_rpc_refuses_malformed_native_authorities_and_bounds() {
    let ctx = context(owner(), Capabilities::default()).await;
    let mut cases = vec![Array::default()];
    for (index, value) in [
        (0, Value::from("v2")),
        (1, "wrong".into()),
        (2, "wrong".into()),
        (3, 0_i64.into()),
        (3, (-1_i64).into()),
        (3, 300_001_i64.into()),
        (3, 1.0_f64.into()),
        (3, "1000".into()),
        (4, Value::Array(Array::default())),
        (4, Value::None),
    ] {
        let mut params = checkpoint();
        params[index] = value;
        cases.push(params);
    }
    for (field, value) in [
        ("key", Value::from("not-native-bytes")),
        ("namespace", "wrong".into()),
        ("version", 2_i64.into()),
        (
            "membership",
            Value::Array(vec![Value::from("not-a-token")].into()),
        ),
        ("unexpected", true.into()),
    ] {
        let Value::Object(mut object) = authority("first").to_rpc_value() else {
            panic!("native object");
        };
        object.insert(field.into(), value);
        let mut params = checkpoint();
        params[4] = Value::Array(vec![Value::Object(object)].into());
        cases.push(params);
    }
    let mut over_count = checkpoint();
    over_count[4] = Value::Array(vec![authority("first").to_rpc_value(); 4097].into());
    cases.push(over_count);
    let mut extra = checkpoint();
    extra.push(Value::None);
    cases.push(extra);
    for version in [1, 2] {
        for params in &cases {
            assert!(invoke(&ctx, version, params.clone()).await.is_err());
        }
    }
}

#[tokio::test]
async fn native_physical_history_rpc_obeys_method_capability_without_fallback() {
    let method = Method::parse_case_sensitive("recall_physical_history_checkpoint");
    let ctx = context(
        owner(),
        Capabilities::default()
            .without_rpc_methods(Targets::Some(HashSet::from([MethodTarget { method }]))),
    )
    .await;
    for version in [1, 2] {
        assert!(matches!(
            invoke(&ctx, version, checkpoint()).await,
            Err(RpcError::MethodNotAllowed)
        ));
    }
}

#[tokio::test]
async fn native_physical_history_sdk_preserves_owned_vector_and_checks_scope() {
    let db = Surreal::new::<Mem>(()).await.unwrap();
    db.use_ns("checkpoint-ns")
        .use_db("checkpoint-db")
        .await
        .unwrap();
    let snapshot = db
        .recall_source_history_inventory("checkpoint-ns", "checkpoint-db", 60_000)
        .await
        .unwrap();
    let checkpoint = db
        .recall_physical_checkpoint_with_history(
            "checkpoint-ns",
            "checkpoint-db",
            60_000,
            bound_authorities(&snapshot),
        )
        .into_owned();
    assert!(!format!("{checkpoint:?}").contains(&"a".repeat(64)));
    checkpoint.await.unwrap();
    for (ns, database, timeout) in [
        ("wrong", "checkpoint-db", 60_000),
        ("checkpoint-ns", "wrong", 60_000),
        ("checkpoint-ns", "checkpoint-db", 0),
        ("checkpoint-ns", "checkpoint-db", 300_001),
    ] {
        assert!(db
            .recall_physical_checkpoint_with_history(
                ns,
                database,
                timeout,
                vec![authority("first")]
            )
            .await
            .is_err());
    }
    assert!(db
        .recall_physical_checkpoint_with_history(
            "checkpoint-ns",
            "checkpoint-db",
            60_000,
            Vec::new()
        )
        .await
        .is_err());
}

#[test]
fn native_physical_history_rpc_request_debug_redacts_authority_payload() {
    let request = surrealdb::rpc::Request {
        id: None,
        version: Some(2),
        method: Method::parse_case_sensitive("recall_physical_history_checkpoint"),
        params: checkpoint(),
    };
    let debug = format!("{request:?}");
    assert!(!debug.contains(&"a".repeat(64)));
    assert!(!debug.contains("11111111-1111-4111-8111-111111111111"));
    assert!(debug.contains("[REDACTED]"));
}

fn inventory_request() -> Array {
    vec![
        Value::from("meerkat-recall-source-history-inventory/v1"),
        "checkpoint-ns".into(),
        "checkpoint-db".into(),
        60_000_i64.into(),
    ]
    .into()
}

async fn inspect(ctx: &Context, version: u8, params: Array) -> Result<Value, RpcError> {
    RpcContext::execute(
        ctx,
        Some(version),
        Method::parse_case_sensitive("recall_source_history_inventory"),
        params,
    )
    .await
    .and_then(|data| Value::try_from(data).map_err(Into::into))
}

#[tokio::test]
#[expect(
    clippy::panic,
    reason = "Unexpected native values are explicit failures of this regression oracle."
)]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
async fn native_history_inventory_rpc_v1_v2_complete_scope_and_read_only_survivor() {
    let ctx = context(owner(), Capabilities::default()).await;
    let created = ctx
        .kvs
        .execute(
            "CREATE survivor:one SET payload = 'original';",
            &owner(),
            None,
        )
        .await
        .unwrap();
    assert!(created[0].result.is_ok());
    let foreign = ctx
        .kvs
        .execute(
            "CREATE survivor:foreign SET payload = 'outside requested scope';",
            &Session::owner().with_ns("foreign-ns").with_db("foreign-db"),
            None,
        )
        .await
        .unwrap();
    assert!(foreign[0].result.is_ok());
    let expected_row = Value::Object(Object::from(BTreeMap::from([
        ("id".to_owned(), Value::Thing(("survivor", "one").into())),
        ("payload".to_owned(), Value::from("original")),
    ])));
    for version in [1, 2] {
        let Value::Object(mut response) =
            inspect(&ctx, version, inventory_request()).await.unwrap()
        else {
            panic!("complete response");
        };
        assert_eq!(response.len(), 5);
        assert_eq!(
            response.remove("protocol"),
            Some(Value::from("meerkat-recall-source-history-inventory/v1"))
        );
        assert_eq!(
            response.remove("namespace"),
            Some(Value::from("checkpoint-ns"))
        );
        assert_eq!(
            response.remove("database"),
            Some(Value::from("checkpoint-db"))
        );
        assert_eq!(response.remove("complete"), Some(Value::Bool(true)));
        let snapshot = surrealdb::method::RecallHistorySnapshot::from_rpc_value(
            response.remove("snapshot").unwrap(),
        )
        .unwrap();
        snapshot.verify().unwrap();
        assert_eq!(
            (snapshot.namespace.as_str(), snapshot.database.as_str()),
            ("checkpoint-ns", "checkpoint-db")
        );
        let mut survivor_count = 0;
        for record in &snapshot.records {
            if let NativeHistoryKey::Record {
                namespace,
                database,
                identity,
            } = record.decoded_key().unwrap()
            {
                assert_eq!(
                    (namespace.as_str(), database.as_str()),
                    ("checkpoint-ns", "checkpoint-db")
                );
                assert_ne!(identity, ("survivor", "foreign").into());
                if identity == ("survivor", "one").into() {
                    survivor_count += 1;
                    assert!(record.current);
                    assert!(!record.tombstone);
                    // Mem is versionless, but its real current records must be
                    // present. A correctly fingerprinted empty snapshot fails.
                    assert_eq!((record.version, record.timestamp), (0, 0));
                    assert_eq!(record.decoded_value().unwrap(), expected_row);
                }
            }
        }
        assert_eq!(survivor_count, 1);
        assert!(response.is_empty());
    }
    let rows = ctx
        .kvs
        .execute("RETURN survivor:one.payload;", &owner(), None)
        .await
        .unwrap();
    assert_eq!(rows[0].result.as_ref().unwrap(), &Value::from("original"));
}

#[tokio::test]
async fn native_history_inventory_rpc_denies_authority_scope_protocol_and_capability() {
    let method = Method::parse_case_sensitive("recall_source_history_inventory");
    let denied = context(
        owner(),
        Capabilities::default()
            .without_rpc_methods(Targets::Some(HashSet::from([MethodTarget { method }]))),
    )
    .await;
    for version in [1, 2] {
        assert!(matches!(
            inspect(&denied, version, inventory_request()).await,
            Err(RpcError::MethodNotAllowed)
        ));
    }
    let mut expired = owner();
    expired.exp = Some(1);
    for session in [
        Session::default()
            .with_ns("checkpoint-ns")
            .with_db("checkpoint-db"),
        Session::for_level(
            Level::Database("checkpoint-ns".into(), "checkpoint-db".into()),
            Role::Owner,
        )
        .with_ns("checkpoint-ns")
        .with_db("checkpoint-db"),
        expired,
        owner().with_db("another"),
        Session::owner(),
    ] {
        let mut ctx = context(session, Capabilities::default()).await;
        ctx.kvs = ctx.kvs.with_auth_enabled(false);
        for version in [1, 2] {
            assert!(inspect(&ctx, version, inventory_request()).await.is_err());
        }
    }
    let ctx = context(owner(), Capabilities::default()).await;
    let mut cases = vec![Array::default()];
    for (index, value) in [
        (0, Value::from("v2")),
        (1, "wrong".into()),
        (2, "wrong".into()),
        (3, 0_i64.into()),
        (3, 300_001_i64.into()),
        (3, 10.0_f64.into()),
        (3, "1000".into()),
    ] {
        let mut params = inventory_request();
        params[index] = value;
        cases.push(params);
    }
    let mut extra = inventory_request();
    extra.push(Value::None);
    cases.push(extra);
    for version in [1, 2] {
        for params in &cases {
            assert!(inspect(&ctx, version, params.clone()).await.is_err());
        }
    }
}

#[tokio::test]
async fn native_history_inventory_sdk_returns_verified_owned_snapshot_and_rejects_scope_change() {
    let db = Surreal::new::<Mem>(()).await.unwrap();
    db.use_ns("checkpoint-ns")
        .use_db("checkpoint-db")
        .await
        .unwrap();
    let snapshot = db
        .recall_source_history_inventory("checkpoint-ns", "checkpoint-db", 60_000)
        .into_owned()
        .await
        .unwrap();
    snapshot.verify().unwrap();
    for (ns, database, timeout) in [
        ("wrong", "checkpoint-db", 60_000),
        ("checkpoint-ns", "wrong", 60_000),
        ("", "checkpoint-db", 60_000),
        ("checkpoint-ns", "checkpoint-db", 0),
        ("checkpoint-ns", "checkpoint-db", 300_001),
    ] {
        assert!(db
            .recall_source_history_inventory(ns, database, timeout)
            .await
            .is_err());
    }
}

#[test]
fn native_history_inventory_response_debug_redacts_raw_records_even_wrong_protocol() {
    let mut fields = Object::default();
    fields.insert("protocol".into(), "incorrect-but-sensitive".into());
    fields.insert("snapshot".into(), "private-source-payload-sentinel".into());
    let data = Data::Other(fields.into());
    let debug = format!("{data:?}");
    assert!(!debug.contains("private-source-payload-sentinel"));
    assert!(debug.contains("[REDACTED]"));
}
