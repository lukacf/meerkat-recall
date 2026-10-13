//! Actual native FOR behavior for owned container values.
//! Fixed native results and durable effects are independent of the move proof.

use super::{array, exact_native, native, nested_data, object, query_value};
use std::collections::BTreeMap;
use surrealdb::dbs::{capabilities::Capabilities, Session};
use surrealdb::error::Db;
use surrealdb::kvs::Datastore;
use surrealdb::sql::{Id, Thing, Value};

// A fixed composite ID stays outside the unchanged identity proof after range
// evaluation. Thus the second descriptor necessarily uses the original
// unconditional v.compute path inside the actual FOR, without a test mode.
fn original_descriptor(data: Value) -> Value {
    object([
        ("data", data),
        (
            "original_evaluator",
            Value::Thing(Thing::from((
                "foreach_original",
                Id::Object(surrealdb::sql::Object::from(
                    BTreeMap::<String, Value>::from([("fixed".into(), 7_i64.into())]),
                )),
            ))),
        ),
    ])
}

async fn actual_for_value(client: &crate::StorageClient, descriptor: Value) -> Value {
    query_value(
        client,
        "FOR $entry IN $input { RETURN $entry.data; };",
        BTreeMap::from([("input".into(), array([descriptor]))]),
    )
    .await
    .expect("actual native FOR returns its first original value")
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
async fn native_foreach_owned_containers_match_original_fallback_and_native_types() {
    let client = native().await;
    let uuid =
        uuid::Uuid::parse_str("12345678-1234-5678-9234-567812345678").expect("fixed native UUID");
    let date = chrono::DateTime::parse_from_rfc3339("2025-01-02T03:04:05.123456789Z")
        .expect("fixed date")
        .with_timezone(&chrono::Utc);
    let values = array([
        Value::None,
        Value::Null,
        true.into(),
        17_i64.into(),
        1.25_f64.into(),
        surrealdb::sql::value("1.250dec").expect("native decimal"),
        "literal:none".into(),
        Value::Bytes(vec![0_u8, 1, 255].into()),
        Value::Uuid(uuid.into()),
        Value::Datetime(surrealdb::sql::Datetime::from(date)),
        Value::Duration(std::time::Duration::from_secs(7).into()),
        Value::Geometry((1.25_f64, 2.5_f64).into()),
        Value::Regex("^(alpha|beta)$".parse().expect("native regex")),
        Value::Thing(Thing::from(("foreach_record", Id::Number(7)))),
        Value::Thing(Thing::from((
            "foreach_record",
            Id::String(uuid.to_string()),
        ))),
        Value::Thing(Thing::from(("foreach_record", Id::Uuid(uuid.into())))),
        array([]),
        object(Vec::<(String, Value)>::new()),
    ]);
    for data in [
        values.clone(),
        object([
            ("native", values),
            ("literal.dot", array([Value::None, Value::Null])),
            ("unicode-å", object([("absent", Value::None)])),
        ]),
    ] {
        let actual = actual_for_value(&client, object([("data", data.clone())])).await;
        let original = actual_for_value(&client, original_descriptor(data.clone())).await;
        exact_native(&actual, &data);
        exact_native(&original, &data);
    }
    // Exercise Array itself as the owned iteration value, not just an Object
    // containing an array. The fixed composite identity forces the old branch.
    let fixed = original_descriptor(Value::None);
    for tail in [Value::None, fixed] {
        let actual = query_value(
            &client,
            "FOR $entry IN $input { RETURN $entry[0]; };",
            BTreeMap::from([("input".into(), array([array([17_i64.into(), tail])]))]),
        )
        .await
        .expect("owned array iteration");
        exact_native(&actual, &17_i64.into());
    }
}

#[tokio::test]
#[expect(
    clippy::literal_string_with_formatting_args,
    reason = "These braces are literal SurrealQL object syntax in the original query fixture."
)]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
async fn native_foreach_owned_containers_keep_caps_empty_ranges_and_loop_control() {
    let client = native().await;
    for width in [4093_i64, 4094, 4095, 4096, 4097, 5000] {
        let data = array((0..width).map(Value::from));
        let actual = actual_for_value(&client, object([("data", data.clone())])).await;
        exact_native(&actual, &data);
    }
    for depth in [0, 29, 30, 31, 32, 33, 64] {
        let data = nested_data(depth, true);
        let actual = actual_for_value(&client, object([("data", data.clone())])).await;
        exact_native(&actual, &data);
    }
    client
        .query_checked("CREATE foreach_trace:control SET labels=[];")
        .await
        .expect("durable loop trace");
    let actual = query_value(
        &client,
        r"LET $entry = {n:99};
        FOR $entry IN [] { THROW 'empty_array_body'; };
        FOR $entry IN 0..0 { THROW 'empty_range_body'; };
        FOR $entry IN [{n:0}, {n:1}, {n:2}, {n:3}] {
            IF $entry.n = 1 { CONTINUE; };
            UPDATE foreach_trace:control SET labels=array::append(labels, $entry.n) RETURN NONE;
            IF $entry.n = 2 { BREAK; };
        };
        FOR $entry IN 20..23 {
            UPDATE foreach_trace:control SET labels=array::append(labels, $entry) RETURN NONE;
        };
        RETURN [$entry.n, (SELECT VALUE labels FROM ONLY foreach_trace:control)];",
        BTreeMap::new(),
    )
    .await
    .expect("original loop scope, empty targets, continue, break and integer range");
    exact_native(
        &actual,
        &array([
            99_i64.into(),
            array([0_i64, 2, 20, 21, 22].map(Value::from)),
        ]),
    );
    client
        .query_checked("UPDATE foreach_trace:control SET labels=[] RETURN NONE;")
        .await
        .expect("fresh nested loop trace");
    let nested = query_value(
        &client,
        r"LET $entry = {n:99};
        FOR $entry IN [{n:10}, {n:20}] {
            LET $outer = $entry.n;
            UPDATE foreach_trace:control SET labels=array::append(labels, $entry.n) RETURN NONE;
            FOR $entry IN [{n:1}, {n:2}] {
                UPDATE foreach_trace:control SET labels=array::append(labels, [$outer, $entry.n]) RETURN NONE;
            };
            UPDATE foreach_trace:control SET labels=array::append(labels, $entry.n) RETURN NONE;
        };
        RETURN [$entry.n, (SELECT VALUE labels FROM ONLY foreach_trace:control)];",
        BTreeMap::new(),
    )
    .await
    .expect("inner iteration shadow ends before the outer and parent scopes resume");
    exact_native(
        &nested,
        &array([
            99_i64.into(),
            array([
                10_i64.into(),
                array([10_i64.into(), 1_i64.into()]),
                array([10_i64.into(), 2_i64.into()]),
                10_i64.into(),
                20_i64.into(),
                array([20_i64.into(), 1_i64.into()]),
                array([20_i64.into(), 2_i64.into()]),
                20_i64.into(),
            ]),
        ]),
    );
    let returned = query_value(
        &client,
        "FOR $entry IN [{n:7}, {n:9}] { RETURN $entry.n; };",
        BTreeMap::new(),
    )
    .await
    .expect("RETURN exits the original FOR");
    exact_native(&returned, &7_i64.into());
    let error = client
        .query_checked("FOR $entry IN 17 { THROW 'invalid_target_body'; };")
        .await
        .expect_err("invalid target refuses before body")
        .to_string();
    assert!(
        error.contains("Can not execute statement using value: 17"),
        "{error}"
    );
    assert!(!error.contains("invalid_target_body"), "{error}");
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
async fn native_foreach_owned_containers_keep_deferred_order_and_first_error() {
    let client = native().await;
    client
        .query_checked(
            r"CREATE foreach_trace:deferred SET labels=[];
        DEFINE FUNCTION fn::foreach_effect($label: string, $value: any) {
            UPDATE foreach_trace:deferred SET labels=array::append(labels, $label) RETURN NONE;
            RETURN $value;
        };",
        )
        .await
        .expect("actual deferred evaluation trace");
    let actual = query_value(
        &client,
        r"BEGIN TRANSACTION;
        FOR $entry IN [
            {n:fn::foreach_effect('range-first', 1)},
            {n:fn::foreach_effect('range-second', 2)}
        ] {
            fn::foreach_effect('body', $entry.n);
        };
        COMMIT TRANSACTION;
        SELECT VALUE labels FROM ONLY foreach_trace:deferred;",
        BTreeMap::new(),
    )
    .await
    .expect("range evaluation completes before original loop bodies");
    exact_native(
        &actual,
        &array(["range-first", "range-second", "body", "body"].map(Value::from)),
    );
    // The first range failure must precede any body and roll back range effects.
    client
        .query_checked(
            "DEFINE FUNCTION fn::foreach_failure() { THROW 'original_foreach_range_failure'; };",
        )
        .await
        .expect("independent original error tag");
    let error = client
        .query_checked(
            r"BEGIN TRANSACTION;
        FOR $entry IN [
            {n:fn::foreach_effect('rolled-back', 3)},
            {n:fn::foreach_failure()}
        ] { THROW 'later_foreach_body'; };
        COMMIT TRANSACTION;",
        )
        .await
        .expect_err("original deferred range failure")
        .to_string();
    assert!(error.contains("original_foreach_range_failure"), "{error}");
    assert!(!error.contains("later_foreach_body"), "{error}");
    let after = query_value(
        &client,
        "SELECT VALUE labels FROM ONLY foreach_trace:deferred;",
        BTreeMap::new(),
    )
    .await
    .expect("durable state after range failure");
    exact_native(&after, &actual);
}

#[tokio::test]
#[expect(
    clippy::literal_string_with_formatting_args,
    reason = "These braces are literal SurrealQL object syntax in the original query fixture."
)]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
async fn native_foreach_owned_containers_keep_duplicate_events_and_atomic_rollback() {
    let client = native().await;
    client
        .query_checked(
            r"CREATE foreach_events:trace SET labels=[];
        CREATE foreach_target:original SET writes=0;
        DEFINE EVENT foreach_update ON TABLE foreach_target WHEN $event='UPDATE' THEN (
            UPDATE foreach_events:trace SET labels=array::append(labels, $after.writes) RETURN NONE
        );",
        )
        .await
        .expect("actual native update event");
    let state = "RETURN [(SELECT VALUE writes FROM ONLY foreach_target:original), (SELECT VALUE labels FROM ONLY foreach_events:trace)];";
    for original_fallback in [false, true] {
        client.query_checked(
            "UPDATE foreach_target:original SET writes=0 RETURN NONE; UPDATE foreach_events:trace SET labels=[] RETURN NONE;",
        ).await.expect("fresh independent event baseline");
        let data = object([(
            "id",
            Value::Thing(Thing::from(("foreach_target", "original"))),
        )]);
        let descriptor = if original_fallback {
            original_descriptor(data)
        } else {
            object([("data", data)])
        };
        client
            .query_bind_checked(
                r"BEGIN TRANSACTION;
            FOR $entry IN $input {
                UPDATE $entry.data.id SET writes += 1;
            };
            COMMIT TRANSACTION;",
                BTreeMap::<String, Value>::from([(
                    "input".into(),
                    array([descriptor.clone(), descriptor.clone(), descriptor]),
                )]),
            )
            .await
            .expect("each duplicate performs its own original update and event");
        let actual = query_value(&client, state, BTreeMap::new())
            .await
            .expect("event trace");
        exact_native(
            &actual,
            &array([3_i64.into(), array([1_i64, 2, 3].map(Value::from))]),
        );
    }
    let before = query_value(&client, state, BTreeMap::new())
        .await
        .expect("actual event state");
    exact_native(
        &before,
        &array([3_i64.into(), array([1_i64, 2, 3].map(Value::from))]),
    );
    let error = client
        .query_checked(
            r"BEGIN TRANSACTION;
        FOR $entry IN [{n:0}, {n:1}, {n:2}] {
            UPDATE foreach_target:original SET writes += 1;
            IF $entry.n = 1 { THROW 'original_foreach_later_failure'; };
        };
        CREATE foreach_after:forbidden;
        COMMIT TRANSACTION;",
        )
        .await
        .expect_err("later original failure aborts all earlier iteration effects")
        .to_string();
    assert!(error.contains("original_foreach_later_failure"), "{error}");
    let after = query_value(&client, state, BTreeMap::new())
        .await
        .expect("rollback state");
    exact_native(&after, &before);
    let sentinel = query_value(
        &client,
        "SELECT * FROM ONLY foreach_after:forbidden;",
        BTreeMap::new(),
    )
    .await
    .expect("read absent later mutation");
    exact_native(&sentinel, &Value::None);
}

#[tokio::test]
#[expect(
    clippy::literal_string_with_formatting_args,
    reason = "These braces are literal SurrealQL object syntax in the original query fixture."
)]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
async fn native_foreach_owned_containers_keep_record_and_global_permissions() {
    let datastore = Box::pin(Datastore::new("memory"))
        .await
        .expect("actual permission datastore")
        .with_auth_enabled(true)
        .with_capabilities(Capabilities::all());
    datastore.bootstrap().await.expect("native node lifecycle");
    let owner = Session::owner()
        .with_ns("foreach_permission")
        .with_db("foreach_permission");
    let setup = Box::pin(datastore.execute(
        r"DEFINE ACCESS reader ON DATABASE TYPE RECORD;
        DEFINE TABLE foreach_reader SCHEMALESS PERMISSIONS FULL;
        CREATE foreach_reader:original;
        DEFINE TABLE foreach_visible SCHEMALESS PERMISSIONS FOR select WHERE allowed=true;
        DEFINE FIELD secret ON foreach_visible TYPE string PERMISSIONS FOR select NONE;
        CREATE foreach_visible:original SET allowed=true, public='visible', secret='hidden';
        DEFINE TABLE foreach_denied SCHEMALESS PERMISSIONS NONE;
        CREATE foreach_denied:original SET public='owner-only';
        DEFINE PARAM $foreach_hidden VALUE 'hidden' PERMISSIONS NONE;",
        &owner,
        None,
    ))
    .await
    .expect("native permission setup");
    for response in setup {
        response.result.expect("permission definition");
    }
    let reader = Session::for_record(
        "foreach_permission",
        "foreach_permission",
        "reader",
        Value::Thing(Thing::from(("foreach_reader", "original"))),
    );
    for (id, expected) in [
        (
            "foreach_visible:original",
            object([
                (
                    "id",
                    Value::Thing(Thing::from(("foreach_visible", "original"))),
                ),
                ("allowed", true.into()),
                ("public", "visible".into()),
            ]),
        ),
        ("foreach_denied:original", Value::None),
    ] {
        let mut responses = Box::pin(datastore.execute(
            &format!("FOR $entry IN [{{id:{id}}}] {{ RETURN (SELECT * FROM ONLY $entry.id); }};"),
            &reader,
            None,
        ))
        .await
        .expect("actual record-session FOR");
        assert_eq!(responses.len(), 1);
        exact_native(
            &responses
                .remove(0)
                .result
                .expect("original native permission selection"),
            &expected,
        );
    }
    let mut responses = Box::pin(datastore.execute(
        "FOR $entry IN [{n:1}] { RETURN $foreach_hidden; };",
        &reader,
        None,
    ))
    .await
    .expect("actual global permission in FOR");
    assert_eq!(responses.len(), 1);
    assert!(
        matches!(responses.remove(0).result, Err(Db::ParamPermissions { name }) if name == "foreach_hidden")
    );
    datastore.shutdown().await.expect("native shutdown");
}
