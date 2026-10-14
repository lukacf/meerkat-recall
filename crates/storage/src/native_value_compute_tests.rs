//! Real native evaluator controls, independent of the computed-data predicate.
#![allow(clippy::expect_used, clippy::panic, clippy::too_many_lines)]

#[path = "native_closure_ownership_tests.rs"]
mod closure_ownership;

#[path = "native_value_dispatch_tests.rs"]
mod dispatch;

#[path = "native_document_dispatch_tests.rs"]
mod document_dispatch;

#[path = "native_missing_field_tests.rs"]
mod missing_fields;

#[path = "native_point_select_tests.rs"]
mod point_select;

#[path = "native_point_select_permission_tests.rs"]
mod point_select_permissions;

#[path = "native_write_preparation_tests.rs"]
mod write_preparation;

#[path = "native_foreach_ownership_tests.rs"]
mod foreach_ownership;

use crate::{StorageClient, StorageResult};
use std::collections::BTreeMap;
use surrealdb::sql::{Array, Id, Object, Param, Thing, Value};

fn object(fields: impl IntoIterator<Item = (impl Into<String>, Value)>) -> Value {
    Value::Object(Object::from(
        fields
            .into_iter()
            .map(|(key, value)| (key.into(), value))
            .collect::<BTreeMap<_, _>>(),
    ))
}

fn array(values: impl IntoIterator<Item = Value>) -> Value {
    Value::Array(Array::from(values.into_iter().collect::<Vec<_>>()))
}

async fn native() -> StorageClient {
    Box::pin(StorageClient::new_memory())
        .await
        .expect("actual native memory evaluator")
}

async fn query_value(
    client: &StorageClient,
    sql: &str,
    bindings: BTreeMap<String, Value>,
) -> StorageResult<Value> {
    let mut response = Box::pin(client.query_bind_checked(sql, bindings)).await?;
    // The SDK's native specialization preserves NONE, Decimal, Things and
    // executable values. A serde/JSON extraction is not this oracle.
    let last = response
        .num_statements()
        .checked_sub(1)
        .expect("actual query must have a statement result");
    let value: surrealdb::Value = response.take(last)?;
    Ok(value.into_inner())
}

async fn evaluate(client: &StorageClient, value: Value) -> StorageResult<Value> {
    query_value(
        client,
        "RETURN $probe;",
        BTreeMap::from([("probe".into(), value)]),
    )
    .await
}

fn exact_native(actual: &Value, expected: &Value) {
    assert_eq!(actual, expected);
    // Native numeric equality can equate different representations. Debug
    // equality additionally checks the concrete native variants, recursively.
    assert_eq!(format!("{actual:?}"), format!("{expected:?}"));
}

// Every nonempty container in this second input contains deferred Param leaves.
// It must therefore use the retained evaluator, not the identity predicate.
// Its leaves resolve to the same separately bound native values. Empty
// containers have no children to evaluate in either path.
fn deferred_leaf_oracle(value: &Value, bindings: &mut BTreeMap<String, Value>) -> Value {
    match value {
        Value::Array(values) => array(
            values
                .iter()
                .map(|value| deferred_leaf_oracle(value, bindings)),
        ),
        Value::Object(fields) => object(
            fields
                .iter()
                .map(|(key, value)| (key.clone(), deferred_leaf_oracle(value, bindings))),
        ),
        leaf => {
            let name = format!("original_leaf_{}", bindings.len());
            bindings.insert(name.clone(), leaf.clone());
            Value::Param(Param::from(name))
        }
    }
}

async fn compare_original_evaluation(client: &StorageClient, value: Value) {
    let mut bindings = BTreeMap::new();
    let original = deferred_leaf_oracle(&value, &mut bindings);
    bindings.insert("probe".into(), original);
    let oracle = query_value(client, "RETURN $probe;", bindings)
        .await
        .expect("actual deferred-leaf original evaluation");
    let actual = evaluate(client, value.clone())
        .await
        .expect("actual native data evaluation");
    exact_native(&oracle, &value);
    exact_native(&actual, &oracle);
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
async fn native_data_identity_matches_deferred_leaf_evaluator_without_type_loss() {
    let client = native().await;
    let decimal = query_value(&client, "RETURN <decimal>7;", BTreeMap::new())
        .await
        .expect("actual native decimal");
    assert!(matches!(
        decimal,
        Value::Number(surrealdb::sql::Number::Decimal(_))
    ));
    let uuid =
        uuid::Uuid::parse_str("8b12ceab-6f6a-4b12-8438-b31a56a3ddbb").expect("fixed native UUID");
    let date = chrono::DateTime::parse_from_rfc3339("2026-10-07T00:00:00Z")
        .expect("fixed native date")
        .with_timezone(&chrono::Utc);
    let values = vec![
        Value::None,
        Value::Null,
        Value::Bool(true),
        Value::from(7_i64),
        Value::from(7.0_f64),
        decimal,
        Value::from("literal source:record text"),
        Value::Bytes(vec![0_u8, 1, 255].into()),
        Value::Uuid(uuid.into()),
        Value::Datetime(surrealdb::sql::Datetime::from(date)),
        Value::Duration(std::time::Duration::from_secs(7).into()),
        Value::Geometry((1.25_f64, 2.5_f64).into()),
        Value::Regex("^(alpha|beta)$".parse().expect("native regex")),
        Value::Thing(Thing::from(("compute_record", Id::Number(7)))),
        Value::Thing(Thing::from((
            "compute_record",
            Id::String(uuid.to_string()),
        ))),
        Value::Thing(Thing::from(("compute_record", Id::Uuid(uuid.into())))),
        array([]),
        object(Vec::<(String, Value)>::new()),
    ];
    let nested = object([
        ("literal.dot", array(values.clone())),
        (
            "literal[0]",
            object([("missing", Value::None), ("null", Value::Null)]),
        ),
        ("unicode-å", array([array(values)])),
    ]);
    compare_original_evaluation(&client, nested.clone()).await;
    compare_original_evaluation(&client, array([nested])).await;
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
async fn native_dynamic_members_still_evaluate_expressions_casts_constants_and_futures() {
    let client = native().await;
    let result = query_value(
        &client,
        "RETURN { arithmetic: 1 + 2, cast: <int>'7', constant: math::pi, future: <future> { 6 * 7 }, nested: { text: string::uppercase('hello'), parameter: $given } };",
        BTreeMap::from([("given".into(), Value::from("original parameter"))]),
    )
    .await
    .expect("deferred members must run through original evaluator");
    exact_native(
        &result,
        &object([
            ("arithmetic", 3_i64.into()),
            ("cast", 7_i64.into()),
            ("constant", std::f64::consts::PI.into()),
            ("future", 42_i64.into()),
            (
                "nested",
                object([
                    ("text", "HELLO".into()),
                    ("parameter", "original parameter".into()),
                ]),
            ),
        ]),
    );
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
async fn native_large_pure_prefix_does_not_skip_or_repeat_deferred_side_effects() {
    let client = native().await;
    client.query_checked(
        "DEFINE TABLE compute_trace SCHEMALESS; DEFINE FUNCTION fn::compute_record_effect($label: string, $value: any) { LET $ordinal = array::len((SELECT id FROM compute_trace)); CREATE compute_trace SET label=$label, ordinal=$ordinal; RETURN $value; };",
    ).await.expect("actual local effect function");
    let prefix = array((0..4097_i64).map(Value::from));
    let probe = object([
        ("a_prefix", prefix.clone()),
        (
            "y_effect",
            surrealdb::sql::value("fn::compute_record_effect('first', 17)")
                .expect("real function AST"),
        ),
        (
            "z_effect",
            surrealdb::sql::value("fn::compute_record_effect('second', 19)")
                .expect("real function AST"),
        ),
    ]);
    // A bound Param does not declare its deferred function's writeability.
    // Original BEGIN creates the write transaction; RETURN retains its value
    // and COMMIT still executes without adding a separate response value.
    let result = query_value(
        &client,
        "BEGIN TRANSACTION; RETURN $probe; COMMIT TRANSACTION;",
        BTreeMap::from([("probe".into(), probe)]),
    )
    .await
    .expect("actual mixed evaluation in the original write transaction");
    exact_native(
        &result,
        &object([
            ("a_prefix", prefix),
            ("y_effect", 17_i64.into()),
            ("z_effect", 19_i64.into()),
        ]),
    );
    let effects = query_value(
        &client,
        "SELECT label, ordinal FROM compute_trace ORDER BY ordinal;",
        BTreeMap::new(),
    )
    .await
    .expect("actual durable effect order");
    exact_native(
        &effects,
        &array([
            object([("label", "first".into()), ("ordinal", 0_i64.into())]),
            object([("label", "second".into()), ("ordinal", 1_i64.into())]),
        ]),
    );
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
async fn native_generated_and_compound_record_ids_keep_original_evaluation() {
    let client = native().await;
    let generated =
        surrealdb::sql::value("compute_record:uuid()").expect("original generated Thing syntax");
    assert!(matches!(&generated, Value::Thing(thing) if matches!(&thing.id, Id::Generate(_))));
    let probe = object([
        ("generated", generated),
        (
            "array_id",
            Value::Thing(Thing::from((
                "compute_record",
                Id::Array(Array::from(vec![
                    Value::Param(Param::from("seed")),
                    7_i64.into(),
                ])),
            ))),
        ),
        (
            "object_id",
            Value::Thing(Thing::from((
                "compute_record",
                Id::Object(Object::from(BTreeMap::<String, Value>::from([(
                    "name".into(),
                    Value::Param(Param::from("seed")),
                )]))),
            ))),
        ),
    ]);
    let mut generated_ids = Vec::new();
    for _ in 0..2 {
        let result = query_value(
            &client,
            "RETURN $probe;",
            BTreeMap::from([
                ("probe".into(), probe.clone()),
                ("seed".into(), "computed seed".into()),
            ]),
        )
        .await
        .expect("actual generated and compound IDs");
        let Value::Object(fields) = result else {
            panic!("native object required")
        };
        let Value::Thing(id) = fields.get("generated").expect("generated field") else {
            panic!("native generated Thing required")
        };
        assert!(matches!(&id.id, Id::Uuid(_)), "generator must be evaluated");
        generated_ids.push(id.clone());
        exact_native(
            fields.get("array_id").expect("array ID"),
            &Value::Thing(Thing::from((
                "compute_record",
                Id::Array(Array::from(vec![
                    Value::from("computed seed"),
                    Value::from(7_i64),
                ])),
            ))),
        );
        exact_native(
            fields.get("object_id").expect("object ID"),
            &Value::Thing(Thing::from((
                "compute_record",
                Id::Object(Object::from(BTreeMap::<String, Value>::from([(
                    "name".into(),
                    "computed seed".into(),
                )]))),
            ))),
        );
    }
    assert_ne!(
        generated_ids[0], generated_ids[1],
        "generator is evaluated on each query"
    );
}

fn nested_data(levels: usize, objects: bool) -> Value {
    let mut value = Value::from(9_i64);
    for _ in 0..levels {
        value = if objects {
            object([("child", value)])
        } else {
            array([value])
        };
    }
    value
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
async fn native_data_depth_limit_and_optimization_caps_preserve_original_results() {
    let client = native().await;
    let maximum =
        usize::try_from(*surrealdb::cnf::MAX_COMPUTATION_DEPTH).expect("native depth fits usize");
    assert!(
        (66..=256).contains(&maximum),
        "qualification requires the original bounded native depth configuration"
    );
    // RETURN $probe consumes one Value dive for the Param, then one per
    // container and one for the final scalar. No environment/depth mutation.
    let boundary = maximum - 2;
    for objects in [false, true] {
        for levels in [0, 31, 32, 33, 64, boundary] {
            let original = nested_data(levels, objects);
            let result = evaluate(&client, original.clone())
                .await
                .expect("original allowed native depth");
            exact_native(&result, &original);
        }
        let error = evaluate(&client, nested_data(boundary + 1, objects))
            .await
            .expect_err("original native depth limit must still refuse");
        assert!(
            error
                .to_string()
                .contains("Reached excessive computation depth"),
            "wrong original depth error: {error}"
        );
    }
    for count in [4095_i64, 4096, 4097, 5000] {
        let original = array((0..count).map(Value::from));
        let result = evaluate(&client, original.clone())
            .await
            .expect("optimization width is not a runtime rejection");
        exact_native(&result, &original);
    }
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
async fn native_deferred_error_order_survives_later_depth_failure() {
    let client = native().await;
    client.query_checked("DEFINE FUNCTION fn::compute_original_failure() { THROW 'original_first_compute_error'; };").await.expect("actual throwing function");
    let failure = surrealdb::sql::value("fn::compute_original_failure()")
        .expect("deferred original error AST");
    let deep = nested_data(
        usize::try_from(*surrealdb::cnf::MAX_COMPUTATION_DEPTH).expect("native depth fits usize"),
        true,
    );
    let error = evaluate(
        &client,
        object([("a_error", failure.clone()), ("z_deep", deep.clone())]),
    )
    .await
    .expect_err("original first error");
    assert!(
        error.to_string().contains("original_first_compute_error"),
        "probe must not reorder first error: {error}"
    );
    let error = evaluate(&client, array([deep, failure]))
        .await
        .expect_err("original first depth error");
    assert!(
        error
            .to_string()
            .contains("Reached excessive computation depth"),
        "original array evaluation order changed: {error}"
    );
    assert!(!error.to_string().contains("original_first_compute_error"));
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
async fn native_identity_data_keeps_original_update_event_and_rollback() {
    let client = native().await;
    client.query_checked("DEFINE TABLE compute_event SCHEMALESS; DEFINE TABLE compute_event_trace SCHEMALESS; DEFINE EVENT original_compute_event ON compute_event WHEN $event='UPDATE' THEN { IF $after.payload.denied { THROW 'original_compute_event_refusal'; }; CREATE compute_event_trace SET nonce=$after.nonce; }; CREATE compute_event:original SET payload={denied:false, values:[1,2,3]}, nonce='initial';").await.expect("actual original event and row");
    client
        .query_checked("UPDATE compute_event:original SET nonce='first';")
        .await
        .expect("actual nonce update executes original event");
    let before = query_value(
        &client,
        "SELECT * FROM ONLY compute_event:original;",
        BTreeMap::new(),
    )
    .await
    .expect("exact beforeimage");
    let error = client
        .query_checked("UPDATE compute_event:original SET payload.denied=true, nonce='refused';")
        .await
        .expect_err("original event still refuses update");
    assert!(error.to_string().contains("original_compute_event_refusal"));
    let after = query_value(
        &client,
        "SELECT * FROM ONLY compute_event:original;",
        BTreeMap::new(),
    )
    .await
    .expect("row after refusal");
    exact_native(&after, &before);
    let events = query_value(
        &client,
        "SELECT VALUE nonce FROM compute_event_trace;",
        BTreeMap::new(),
    )
    .await
    .expect("actual event effects");
    exact_native(&events, &array(["first".into()]));
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
async fn native_local_projection_matches_original_evaluation_and_exact_native_values() {
    let client = native().await;
    let uuid =
        uuid::Uuid::parse_str("8b12ceab-6f6a-4b12-8438-b31a56a3ddbb").expect("fixed native UUID");
    let picked = object([
        ("none", Value::None),
        ("null", Value::Null),
        ("bytes", Value::Bytes(vec![0_u8, 255].into())),
        ("regex", Value::Regex("^native$".parse().expect("regex"))),
        ("float", Value::from(7.0_f64)),
        ("integer", Value::from(7_i64)),
        (
            "decimal",
            query_value(&client, "RETURN <decimal>7;", BTreeMap::new())
                .await
                .expect("native decimal"),
        ),
        (
            "uuid_id",
            Value::Thing(Thing::from(("projection", Id::Uuid(uuid.into())))),
        ),
        (
            "string_id",
            Value::Thing(Thing::from(("projection", Id::String(uuid.to_string())))),
        ),
    ]);
    let object_probe = object([
        ("pick", picked.clone()),
        ("literal.dot", array([Value::None, Value::from(7.0_f64)])),
        ("unselected", array((0..256_i64).map(Value::from))),
    ]);
    for (probe, sql, expected) in [
        (object_probe.clone(), "RETURN $probe.pick;", picked.clone()),
        (
            object_probe.clone(),
            "RETURN $probe.`literal.dot`[1];",
            Value::from(7.0_f64),
        ),
        (
            object_probe.clone(),
            "RETURN $probe.`literal.dot`.len();",
            Value::from(2_i64),
        ),
        (object_probe.clone(), "RETURN $probe.absent;", Value::None),
        (
            array([Value::None, object_probe]),
            "RETURN $probe[1].pick;",
            picked,
        ),
    ] {
        let mut bindings = BTreeMap::new();
        let original = deferred_leaf_oracle(&probe, &mut bindings);
        bindings.insert("probe".into(), original);
        let oracle = query_value(&client, sql, bindings)
            .await
            .expect("original deferred-leaf projection");
        let actual = query_value(&client, sql, BTreeMap::from([("probe".into(), probe)]))
            .await
            .expect("actual local native projection");
        exact_native(&oracle, &expected);
        exact_native(&actual, &oracle);
    }
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
async fn native_local_projection_keeps_unselected_effects_and_dynamic_path_order() {
    let client = native().await;
    client.query_checked(
        "DEFINE TABLE projection_trace SCHEMALESS; DEFINE FUNCTION fn::projection_effect($label: string, $value: any) { LET $ordinal = array::len((SELECT id FROM projection_trace)); CREATE projection_trace SET label=$label, ordinal=$ordinal; RETURN $value; };",
    ).await.expect("actual local projection effects");
    // The first two inputs must evaluate unselected siblings before the path;
    // the third is pure and eligible, but its dynamic path still must execute.
    for (deferred, width) in [(true, 0_i64), (true, 4097), (false, 256)] {
        client
            .query_checked("DELETE projection_trace;")
            .await
            .expect("reset actual trace");
        let mut fields = BTreeMap::<String, Value>::from([
            ("a_prefix".into(), array((0..width).map(Value::from))),
            ("pick".into(), Value::from(17_i64)),
        ]);
        if deferred {
            fields.insert(
                "b_effect".into(),
                surrealdb::sql::value("fn::projection_effect('first', 1)")
                    .expect("first effect AST"),
            );
            fields.insert(
                "z_effect".into(),
                surrealdb::sql::value("fn::projection_effect('second', 2)")
                    .expect("second effect AST"),
            );
        }
        let result = query_value(
            &client,
            "BEGIN TRANSACTION; RETURN $probe[$field]; COMMIT TRANSACTION;",
            BTreeMap::from([
                ("probe".into(), Value::Object(Object::from(fields))),
                (
                    "field".into(),
                    surrealdb::sql::value("fn::projection_effect('path', 'pick')")
                        .expect("path effect AST"),
                ),
            ]),
        )
        .await
        .expect("actual projection in original write transaction");
        exact_native(&result, &Value::from(17_i64));
        let trace = query_value(
            &client,
            "SELECT label, ordinal FROM projection_trace ORDER BY ordinal;",
            BTreeMap::new(),
        )
        .await
        .expect("actual durable ordered effects");
        let labels: &[&str] = if deferred {
            &["first", "second", "path"]
        } else {
            &["path"]
        };
        let expected = array(labels.iter().enumerate().map(|(ordinal, label)| {
            object([
                ("label", Value::from(*label)),
                (
                    "ordinal",
                    Value::from(i64::try_from(ordinal).expect("small ordinal")),
                ),
            ])
        }));
        exact_native(&trace, &expected);
    }
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
async fn native_local_projection_checks_unselected_depth_before_field_lookup() {
    let client = native().await;
    let maximum =
        usize::try_from(*surrealdb::cnf::MAX_COMPUTATION_DEPTH).expect("native depth fits usize");
    assert!((66..=256).contains(&maximum));
    // Idiom, start Param, bound Object, and the innermost scalar each consume
    // one original Value dive. A shallow selected field cannot hide a deeper
    // unselected sibling, including at the exact original depth boundary.
    let boundary = maximum - 4;
    for levels in [0, 30, 31, 32, 64, boundary] {
        let probe = object([
            ("pick", Value::from(17_i64)),
            ("unselected", nested_data(levels, true)),
        ]);
        let result = query_value(
            &client,
            "RETURN $probe.pick;",
            BTreeMap::from([("probe".into(), probe)]),
        )
        .await
        .expect("original allowed unselected depth");
        exact_native(&result, &Value::from(17_i64));
    }
    let probe = object([
        ("pick", Value::from(17_i64)),
        ("unselected", nested_data(boundary + 1, true)),
    ]);
    let error = query_value(
        &client,
        "RETURN $probe.pick;",
        BTreeMap::from([("probe".into(), probe)]),
    )
    .await
    .expect_err("unselected depth must still refuse before lookup");
    assert!(
        error
            .to_string()
            .contains("Reached excessive computation depth"),
        "wrong depth error: {error}"
    );
    for width in [4094_i64, 4095, 4096, 4097, 5000] {
        let probe = object([
            ("pick", Value::from(17_i64)),
            ("unselected", array((0..width).map(Value::from))),
        ]);
        let result = query_value(
            &client,
            "RETURN $probe.pick;",
            BTreeMap::from([("probe".into(), probe)]),
        )
        .await
        .expect("probe cap is not a query limit");
        exact_native(&result, &Value::from(17_i64));
    }
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
async fn native_local_projection_preserves_unselected_first_error_and_record_id_evaluation() {
    let client = native().await;
    client
        .query_checked(
            "DEFINE FUNCTION fn::projection_failure() { THROW 'original_projection_failure'; };",
        )
        .await
        .expect("actual original failure");
    let failure = surrealdb::sql::value("fn::projection_failure()").expect("failure AST");
    let deep = nested_data(
        usize::try_from(*surrealdb::cnf::MAX_COMPUTATION_DEPTH).expect("depth"),
        true,
    );
    for (probe, expected) in [
        (
            object([
                ("a_error", failure.clone()),
                ("pick", Value::from(17_i64)),
                ("z_deep", deep.clone()),
            ]),
            "original_projection_failure",
        ),
        (
            object([
                ("a_deep", deep),
                ("pick", Value::from(17_i64)),
                ("z_error", failure),
            ]),
            "Reached excessive computation depth",
        ),
    ] {
        let error = query_value(
            &client,
            "RETURN $probe.pick;",
            BTreeMap::from([("probe".into(), probe)]),
        )
        .await
        .expect_err("original unselected first error");
        assert!(
            error.to_string().contains(expected),
            "wrong original first error: {error}"
        );
    }
    // A compound ID's deferred component must run even though another field
    // is selected. Generated IDs must also resolve anew on each projection.
    let compound = Value::Thing(Thing::from((
        "projection",
        Id::Array(Array::from(vec![surrealdb::sql::value(
            "fn::projection_failure()",
        )
        .expect("compound failure AST")])),
    )));
    let error = query_value(
        &client,
        "RETURN $probe.pick;",
        BTreeMap::from([(
            "probe".into(),
            object([("pick", Value::from(17_i64)), ("unselected", compound)]),
        )]),
    )
    .await
    .expect_err("unselected compound ID must evaluate");
    assert!(error.to_string().contains("original_projection_failure"));
    let mut ids = Vec::new();
    for _ in 0..2 {
        let result = query_value(
            &client,
            "RETURN $probe.generated;",
            BTreeMap::from([(
                "probe".into(),
                object([
                    (
                        "generated",
                        surrealdb::sql::value("projection:uuid()").expect("generated ID AST"),
                    ),
                    ("unselected", Value::from(17_i64)),
                ]),
            )]),
        )
        .await
        .expect("actual generated projection");
        assert!(matches!(&result, Value::Thing(record) if matches!(&record.id, Id::Uuid(_))));
        ids.push(result);
    }
    assert_ne!(ids[0], ids[1]);
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
async fn native_local_projection_preserves_special_global_and_live_record_paths() {
    let client = native().await;
    let shadows = BTreeMap::from([
        (
            "this".into(),
            object([("pick", Value::from("wrong local this"))]),
        ),
        (
            "self".into(),
            object([("pick", Value::from("wrong local self"))]),
        ),
    ]);
    let absent = query_value(&client, "RETURN [$this.pick, $self.pick];", shadows.clone())
        .await
        .expect("special parameters without current document");
    exact_native(&absent, &array([Value::None, Value::None]));
    let document = query_value(
        &client,
        "SELECT VALUE [$this.pick, $self.pick] FROM {pick:'document'};",
        shadows,
    )
    .await
    .expect("special parameters use current document");
    exact_native(
        &document,
        &array([array([Value::from("document"), Value::from("document")])]),
    );
    client.query_checked("DEFINE PARAM $projection_global VALUE {pick:'global'}; DEFINE TABLE projection_live SCHEMALESS; CREATE projection_live:original SET pick='first';")
        .await.expect("actual global parameter and live row");
    for (bindings, expected) in [
        (BTreeMap::new(), Value::from("global")),
        (
            BTreeMap::from([(
                "projection_global".into(),
                object([("pick", Value::from("local"))]),
            )]),
            Value::from("local"),
        ),
    ] {
        let actual = query_value(&client, "RETURN $projection_global.pick;", bindings)
            .await
            .expect("original global resolution and local shadow");
        exact_native(&actual, &expected);
    }
    let missing = query_value(
        &client,
        "RETURN $projection_not_defined.pick;",
        BTreeMap::new(),
    )
    .await
    .expect("original missing global result");
    exact_native(&missing, &Value::None);
    let probe = object([(
        "link",
        Value::Thing(Thing::from((
            "projection_live",
            Id::String("original".into()),
        ))),
    )]);
    for expected in ["first", "second"] {
        if expected == "second" {
            client
                .query_checked("UPDATE projection_live:original SET pick='second';")
                .await
                .expect("actual intervening row update");
        }
        let actual = query_value(
            &client,
            "RETURN $probe.link.pick;",
            BTreeMap::from([("probe".into(), probe.clone())]),
        )
        .await
        .expect("original live Thing traversal after native projection");
        exact_native(&actual, &Value::from(expected));
    }

    // Exercise actual global permissions with a record-level session. This
    // uses the same public native evaluator, with auth enabled explicitly;
    // StorageClient's privileged local session cannot prove this refusal.
    let datastore = Box::pin(surrealdb::kvs::Datastore::new("memory"))
        .await
        .expect("actual permission datastore")
        .with_auth_enabled(true)
        .with_capabilities(surrealdb::dbs::capabilities::Capabilities::all());
    let owner = surrealdb::dbs::Session::owner()
        .with_ns("projection")
        .with_db("projection");
    let setup = Box::pin(datastore.execute(
        "DEFINE PARAM $visible VALUE {pick:'visible'} PERMISSIONS FULL; DEFINE PARAM $hidden VALUE {pick:'hidden'} PERMISSIONS NONE;",
        &owner,
        None,
    ))
    .await
    .expect("actual permission definitions");
    for response in setup {
        response.result.expect("definition succeeds");
    }
    let record = surrealdb::dbs::Session::for_record(
        "projection",
        "projection",
        "user",
        Value::Thing(Thing::from(("user", Id::String("original".into())))),
    );
    let mut responses =
        Box::pin(datastore.execute("RETURN $visible.pick; RETURN $hidden.pick;", &record, None))
            .await
            .expect("actual record-session projections");
    assert_eq!(responses.len(), 2);
    let refused = responses.pop().expect("hidden response").result;
    assert!(
        matches!(refused, Err(surrealdb::error::Db::ParamPermissions { name }) if name == "hidden")
    );
    let visible = responses
        .pop()
        .expect("visible response")
        .result
        .expect("visible permission");
    exact_native(&visible, &Value::from("visible"));
}
