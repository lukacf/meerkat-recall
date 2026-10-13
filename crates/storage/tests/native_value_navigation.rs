//! Native path evaluation controls. Dynamic string indexing supplies the
//! original evaluator branch independently of ordinary Object/Field traversal.
#![allow(clippy::expect_used, clippy::panic, clippy::too_many_lines)]

use std::collections::BTreeMap;
use surrealdb::engine::local::{Db, Mem};
use surrealdb::sql::{Array, Id, Idiom, Object, Part, Thing, Value};
use surrealdb::Surreal;

async fn native() -> Surreal<Db> {
    let db = Surreal::new::<Mem>(()).await.expect("native database");
    db.use_ns("value-navigation")
        .use_db("value-navigation")
        .await
        .expect("native scope");
    db
}

fn object(fields: impl IntoIterator<Item = (&'static str, Value)>) -> Value {
    Value::Object(Object::from(
        fields
            .into_iter()
            .map(|(name, value)| (name.to_owned(), value))
            .collect::<BTreeMap<_, _>>(),
    ))
}

async fn value(
    db: &Surreal<Db>,
    sql: &str,
    bindings: BTreeMap<String, Value>,
) -> surrealdb::Result<Value> {
    let mut response = db.query(sql).bind(bindings).await?.check()?;
    let last = response
        .num_statements()
        .checked_sub(1)
        .expect("query has a result");
    let result: surrealdb::Value = response.take(last)?;
    Ok(result.into_inner())
}

fn exact(actual: &Value, expected: &Value) {
    assert_eq!(actual, expected);
    // Numeric equality alone can hide a changed native representation.
    assert_eq!(format!("{actual:?}"), format!("{expected:?}"));
}

#[tokio::test]
async fn native_object_fields_match_dynamic_paths_and_keep_native_values() {
    let db = native().await;
    let selected = object([
        ("none", Value::None),
        ("null", Value::Null),
        ("integer", 7_i64.into()),
        ("float", 7.0_f64.into()),
        ("bytes", Value::Bytes(vec![0_u8, 255].into())),
        ("record", Thing::from(("navigation", Id::Number(7))).into()),
    ]);
    let probe = object([(
        "outer",
        object([
            ("selected", selected.clone()),
            ("literal.dot", 19_i64.into()),
            (
                "items",
                Value::Array(Array::from(vec![Value::from(3_i64), Value::from(5_i64)])),
            ),
        ]),
    )]);
    for (ordinary, dynamic, expected) in [
        (
            "RETURN $probe.outer.selected;",
            "RETURN $probe['outer']['selected'];",
            selected,
        ),
        (
            "RETURN $probe.outer.`literal.dot`;",
            "RETURN $probe['outer']['literal.dot'];",
            19_i64.into(),
        ),
        (
            "RETURN $probe.outer.absent;",
            "RETURN $probe['outer']['absent'];",
            Value::None,
        ),
        (
            "RETURN $probe.outer.items[1];",
            "RETURN $probe['outer']['items'][1];",
            5_i64.into(),
        ),
        (
            "RETURN $probe.outer.items.len();",
            "RETURN $probe['outer']['items'].len();",
            2_i64.into(),
        ),
    ] {
        for query in [ordinary, dynamic] {
            let result = value(
                &db,
                query,
                BTreeMap::from([("probe".into(), probe.clone())]),
            )
            .await
            .expect("actual native path");
            exact(&result, &expected);
        }
    }
}

#[tokio::test]
async fn native_object_prefix_keeps_complex_ids_arrays_and_remote_record_reads() {
    let db = native().await;
    db.query("CREATE navigation_remote:row SET nested={answer: 47} RETURN NONE;")
        .await
        .expect("remote fixture response")
        .check()
        .expect("remote fixture");
    let object_id = Thing::from((
        "navigation_id",
        Id::Object(Object::from(BTreeMap::from([(
            String::from("nested"),
            object([("answer", 41_i64.into())]),
        )]))),
    ));
    let array_id = Thing::from((
        "navigation_id",
        Id::Array(Array::from(vec![object([("answer", 43_i64.into())])])),
    ));
    let probe = object([(
        "outer",
        object([
            ("object_row", object([("id", object_id.into())])),
            ("array_row", object([("id", array_id.into())])),
            ("link", Thing::from(("navigation_remote", "row")).into()),
            (
                "rows",
                Value::Array(Array::from(vec![
                    object([("answer", 11_i64.into())]),
                    object([("answer", 13_i64.into())]),
                ])),
            ),
        ]),
    )]);
    for (query, expected) in [
        (
            "RETURN $probe.outer.object_row.id.nested.answer;",
            41_i64.into(),
        ),
        ("RETURN $probe.outer.array_row.id[0].answer;", 43_i64.into()),
        ("RETURN $probe.outer.link.nested.answer;", 47_i64.into()),
        (
            "RETURN $probe.outer.rows.answer;",
            Value::Array(Array::from(vec![Value::from(11_i64), Value::from(13_i64)])),
        ),
    ] {
        let result = value(
            &db,
            query,
            BTreeMap::from([("probe".into(), probe.clone())]),
        )
        .await
        .expect("native special path after object prefix");
        exact(&result, &expected);
    }
    db.query("UPDATE navigation_remote:row SET nested.answer=53 RETURN NONE;")
        .await
        .expect("remote change response")
        .check()
        .expect("remote change");
    exact(
        &value(
            &db,
            "RETURN $probe.outer.link.nested.answer;",
            BTreeMap::from([("probe".into(), probe)]),
        )
        .await
        .expect("fresh remote read"),
        &53_i64.into(),
    );
}

#[tokio::test]
async fn native_object_prefix_replays_stored_futures_and_rolls_back_effects() {
    let db = native().await;
    db.query(
        "CREATE navigation_counter:row SET reads=0 RETURN NONE;
         DEFINE FUNCTION fn::navigation_read() {
             UPDATE navigation_counter:row SET reads+=1 RETURN NONE;
             RETURN {answer: (SELECT VALUE reads FROM ONLY navigation_counter:row)};
         };
         CREATE navigation_future:row SET
             outer={deferred: <future> { fn::navigation_read() }} RETURN NONE;",
    )
    .await
    .expect("stored future fixture response")
    .check()
    .expect("stored future fixture");
    let count = "SELECT VALUE reads FROM ONLY navigation_counter:row;";
    exact(
        &value(&db, count, BTreeMap::new())
            .await
            .expect("initial reads"),
        &0_i64.into(),
    );
    // A deferred value in the middle of a path must run before its child is
    // selected. Both ordinary and original dynamic paths perform a fresh read.
    for (query, expected) in [
        (
            "BEGIN TRANSACTION; SELECT VALUE outer.deferred.answer FROM ONLY navigation_future:row; COMMIT TRANSACTION;",
            1_i64,
        ),
        (
            "BEGIN TRANSACTION; SELECT VALUE outer['deferred']['answer'] FROM ONLY navigation_future:row; COMMIT TRANSACTION;",
            2,
        ),
        (
            "BEGIN TRANSACTION; SELECT VALUE outer.deferred.answer FROM ONLY navigation_future:row; COMMIT TRANSACTION;",
            3,
        ),
    ] {
        exact(
            &value(&db, query, BTreeMap::new())
                .await
                .expect("stored future navigation"),
            &expected.into(),
        );
    }
    // An empty remaining path clones the Future; the unchanged idiom compute
    // step must still evaluate that returned value exactly once.
    exact(
        &value(
            &db,
            "BEGIN TRANSACTION; SELECT VALUE outer.deferred FROM ONLY navigation_future:row; COMMIT TRANSACTION;",
            BTreeMap::new(),
        )
        .await
        .expect("terminal stored future"),
        &object([("answer", 4_i64.into())]),
    );
    let mut response = db
        .query(
            "BEGIN TRANSACTION;
             UPDATE navigation_future:row SET seen=outer.deferred.answer RETURN NONE;
             THROW 'navigation_rollback_' + <string>(SELECT VALUE reads FROM ONLY navigation_counter:row);
             COMMIT TRANSACTION;",
        )
        .await
        .expect("rollback response");
    let errors = response.take_errors();
    assert_eq!(errors.len(), 2);
    assert!(matches!(
        errors.get(&0),
        Some(surrealdb::Error::Db(surrealdb::error::Db::QueryNotExecuted))
    ));
    assert!(matches!(
        errors.get(&1),
        Some(surrealdb::Error::Db(surrealdb::error::Db::Thrown(message)))
            if message == "navigation_rollback_5"
    ));
    exact(
        &value(&db, count, BTreeMap::new())
            .await
            .expect("reads after rollback"),
        &4_i64.into(),
    );
    exact(
        &value(
            &db,
            "SELECT VALUE seen FROM ONLY navigation_future:row;",
            BTreeMap::new(),
        )
        .await
        .expect("row after rollback"),
        &Value::None,
    );
}

#[tokio::test]
async fn native_object_prefix_keeps_dynamic_path_effect_order_and_first_error() {
    let db = native().await;
    db.query(
        "CREATE navigation_path_trace:row SET labels=[] RETURN NONE;
         DEFINE FUNCTION fn::navigation_part($label: string, $part: string) {
             UPDATE navigation_path_trace:row SET labels += [$label] RETURN NONE;
             RETURN $part;
         };
         DEFINE FUNCTION fn::navigation_fail() { THROW 'navigation_first_error'; };",
    )
    .await
    .expect("path fixture response")
    .check()
    .expect("path fixture");
    let probe = object([(
        "outer",
        object([("inner", object([("answer", 17_i64.into())]))]),
    )]);
    let bindings = BTreeMap::from([("probe".into(), probe)]);
    exact(
        &value(
            &db,
            "BEGIN TRANSACTION;
             RETURN $probe.outer[fn::navigation_part('first', 'inner')]
                 [fn::navigation_part('second', 'answer')];
             COMMIT TRANSACTION;",
            bindings.clone(),
        )
        .await
        .expect("dynamic path effects"),
        &17_i64.into(),
    );
    exact(
        &value(
            &db,
            "SELECT VALUE labels FROM ONLY navigation_path_trace:row;",
            BTreeMap::new(),
        )
        .await
        .expect("path effect order"),
        &Value::Array(Array::from(vec!["first", "second"])),
    );
    exact(
        &value(
            &db,
            "BEGIN TRANSACTION;
             RETURN $probe.outer.absent[fn::navigation_part('missing_must_not_run', 'answer')];
             COMMIT TRANSACTION;",
            bindings.clone(),
        )
        .await
        .expect("missing field keeps original dynamic suffix behavior"),
        &Value::None,
    );
    let error = value(
        &db,
        "BEGIN TRANSACTION;
         RETURN $probe.outer[fn::navigation_fail()]
             [fn::navigation_part('must_not_run', 'answer')];
         COMMIT TRANSACTION;",
        bindings,
    )
    .await
    .expect_err("original first dynamic path error");
    assert!(matches!(
        error,
        surrealdb::Error::Db(surrealdb::error::Db::Thrown(message))
            if message == "navigation_first_error"
    ));
    exact(
        &value(
            &db,
            "SELECT VALUE labels FROM ONLY navigation_path_trace:row;",
            BTreeMap::new(),
        )
        .await
        .expect("failed dynamic path keeps trace"),
        &Value::Array(Array::from(vec!["first", "second"])),
    );
}

#[tokio::test]
async fn native_navigation_checks_original_full_path_limit_before_missing_field() {
    let db = native().await;
    let maximum =
        usize::try_from(*surrealdb::cnf::MAX_COMPUTATION_DEPTH).expect("native path limit");
    assert!((66..=256).contains(&maximum));
    // Construct the native AST directly, independently of the SQL parser's own
    // depth limit. The absent first field must not hide an overlong suffix.
    for length in [maximum - 1, maximum, maximum + 1] {
        let mut parts = vec![Part::Start(object([]))];
        parts.extend((0..length).map(|_| Part::Field("absent".into())));
        let result = value(
            &db,
            "RETURN $probe;",
            BTreeMap::from([("probe".into(), Value::Idiom(Idiom::from(parts)))]),
        )
        .await;
        if length <= maximum {
            exact(&result.expect("original allowed path length"), &Value::None);
        } else {
            assert!(matches!(
                result,
                Err(surrealdb::Error::Db(
                    surrealdb::error::Db::ComputationDepthExceeded
                ))
            ));
        }
    }
}

#[tokio::test]
async fn native_object_entries_preserves_order_and_nested_native_values() {
    let db = native().await;
    let uuid = Value::Uuid(
        uuid::Uuid::parse_str("00000000-0000-0000-0000-000000000017")
            .expect("fixed native UUID")
            .into(),
    );
    let nested = object([(
        "values",
        Value::Array(Array::from(vec![
            Value::None,
            Value::Null,
            7_i64.into(),
            7.0_f64.into(),
            Value::Bytes(vec![0_u8, 255].into()),
            Thing::from(("entries", Id::Number(7))).into(),
            Thing::from(("entries", Id::String("7".into()))).into(),
            Thing::from((
                "entries",
                Id::Object(Object::from(BTreeMap::from([(
                    "nested".to_owned(),
                    Value::Array(Array::from(vec![uuid.clone(), Value::None])),
                )]))),
            ))
            .into(),
        ])),
    )]);
    let literal = Value::Bytes(vec![17_u8, 0, 255].into());
    let probe = object([
        ("é", uuid.clone()),
        ("z", nested.clone()),
        ("a.b", literal.clone()),
        ("", Value::None),
    ]);
    // The expected sequence is explicit: dotted keys remain literal and
    // native values keep their representations inside each two-value entry.
    let expected = Value::Array(Array::from(vec![
        Value::Array(Array::from(vec![Value::from(""), Value::None])),
        Value::Array(Array::from(vec![Value::from("a.b"), literal])),
        Value::Array(Array::from(vec![Value::from("z"), nested])),
        Value::Array(Array::from(vec![Value::from("é"), uuid])),
    ]));
    for (probe, expected) in [
        (object([]), Value::Array(Array::from(Vec::<Value>::new()))),
        (probe, expected),
    ] {
        exact(
            &value(
                &db,
                "RETURN object::entries($probe);",
                BTreeMap::from([("probe".into(), probe)]),
            )
            .await
            .expect("actual object entries"),
            &expected,
        );
    }
}

#[tokio::test]
async fn native_object_entries_evaluates_argument_once_and_keeps_transaction_errors() {
    let db = native().await;
    db.query(
        "CREATE entries_counter:row SET calls=0 RETURN NONE;
         DEFINE FUNCTION fn::entries_once() {
             UPDATE entries_counter:row SET calls+=1 RETURN NONE;
             RETURN {result: (SELECT VALUE calls FROM ONLY entries_counter:row)};
         };
         DEFINE FUNCTION fn::entries_fail() {
             UPDATE entries_counter:row SET calls+=1 RETURN NONE;
             THROW 'entries_argument_failure';
         };",
    )
    .await
    .expect("entries argument fixture response")
    .check()
    .expect("entries argument fixture");
    exact(
        &value(
            &db,
            "BEGIN TRANSACTION; RETURN object::entries(fn::entries_once()); COMMIT TRANSACTION;",
            BTreeMap::new(),
        )
        .await
        .expect("evaluated entries argument"),
        &Value::Array(Array::from(vec![Value::Array(Array::from(vec![
            Value::from("result"),
            1_i64.into(),
        ]))])),
    );
    let count = "SELECT VALUE calls FROM ONLY entries_counter:row;";
    exact(
        &value(&db, count, BTreeMap::new())
            .await
            .expect("exactly one argument call"),
        &1_i64.into(),
    );
    let mut response = db
        .query("BEGIN TRANSACTION; RETURN object::entries(fn::entries_fail()); COMMIT TRANSACTION;")
        .await
        .expect("failed argument response");
    let errors = response.take_errors();
    assert!(errors.values().any(|error| matches!(
        error,
        surrealdb::Error::Db(surrealdb::error::Db::Thrown(message))
            if message == "entries_argument_failure"
    )));
    exact(
        &value(&db, count, BTreeMap::new())
            .await
            .expect("argument write rolled back"),
        &1_i64.into(),
    );
}

#[tokio::test]
async fn native_none_cleanup_preserves_array_slots_and_opaque_native_values() {
    let db = native().await;
    let opaque: Value = Thing::from((
        "cleanup_reference",
        Id::Object(Object::from(BTreeMap::<String, Value>::from([
            ("absent".into(), Value::None),
            ("slots".into(), Value::Array(Array::from(vec![Value::None]))),
        ]))),
    ))
    .into();
    let clean_leaf = object([
        ("", Value::Null),
        ("0", 7_i64.into()),
        ("literal.dot", Value::Bytes(vec![0_u8, 255].into())),
        ("reference", opaque.clone()),
    ]);
    let mut dirty_leaf = clean_leaf.clone();
    if let Value::Object(fields) = &mut dirty_leaf {
        fields.insert("remove.me".into(), Value::None);
    }
    let nested = |leaf| {
        object([
            ("empty_object", object([])),
            (
                "empty_array",
                Value::Array(Array::from(Vec::<Value>::new())),
            ),
            ("reference", opaque.clone()),
            (
                "levels",
                Value::Array(Array::from(vec![
                    Value::None,
                    Value::Null,
                    Value::Array(Array::from(vec![
                        Value::None,
                        Value::Array(Array::from(vec![leaf, Value::None])),
                    ])),
                ])),
            ),
        ])
    };
    let clean = nested(clean_leaf);
    // Expected postimages are explicit; the native cleanup implementation is
    // not used to calculate them. Repeated clean writes exercise its no-op path.
    for (probe, expected) in [
        (nested(dirty_leaf), clean.clone()),
        (clean.clone(), clean),
        (opaque.clone(), opaque),
        (Value::Null, Value::Null),
        (Value::None, Value::None),
        (41_i64.into(), 41_i64.into()),
        (object([]), object([])),
        (
            Value::Array(Array::from(vec![Value::None])),
            Value::Array(Array::from(vec![Value::None])),
        ),
    ] {
        exact(
            &value(
                &db,
                "UPSERT cleanup_shapes:row SET payload=$probe RETURN NONE;
                 SELECT VALUE payload FROM ONLY cleanup_shapes:row;",
                BTreeMap::from([("probe".into(), probe)]),
            )
            .await
            .expect("native cleanup postimage"),
            &expected,
        );
    }
}

#[tokio::test]
async fn native_none_cleanup_fallback_keeps_large_and_deep_postimages() {
    let db = native().await;
    let wide_fields: BTreeMap<String, Value> = (0..4100)
        .map(|index| (format!("field_{index:04}"), Value::from(i64::from(index))))
        .collect();
    let wide = Value::Object(Object::from(wide_fields.clone()));
    let mut dirty_wide = wide_fields;
    // The first removable value is beyond the borrowing proof's node budget.
    dirty_wide.insert("zz_remove".into(), Value::None);
    let mut deep = object([("retained", Value::Null)]);
    let mut dirty_deep = object([("retained", Value::Null), ("remove", Value::None)]);
    for _ in 0..40 {
        deep = object([("next", deep)]);
        dirty_deep = object([("next", dirty_deep)]);
    }
    for (probe, expected) in [
        (wide.clone(), wide.clone()),
        (Value::Object(Object::from(dirty_wide)), wide),
        (deep.clone(), deep.clone()),
        (dirty_deep, deep),
    ] {
        exact(
            &value(
                &db,
                "UPSERT cleanup_fallback:row SET payload=$probe RETURN NONE;
                 SELECT VALUE payload FROM ONLY cleanup_fallback:row;",
                BTreeMap::from([("probe".into(), probe)]),
            )
            .await
            .expect("complete original cleanup after bounded proof"),
            &expected,
        );
    }
}

#[tokio::test]
async fn native_none_cleanup_keeps_field_rules_events_and_strict_errors() {
    for strict in [false, true] {
        let db = Surreal::new::<Mem>(surrealdb::opt::Config::default().set_strict(strict))
            .await
            .expect("native schema database");
        db.query("DEFINE NAMESPACE cleanup_schema;")
            .await
            .expect("namespace definition response")
            .check()
            .expect("namespace definition");
        db.use_ns("cleanup_schema").await.expect("schema namespace");
        db.query("DEFINE DATABASE cleanup_schema;")
            .await
            .expect("database definition response")
            .check()
            .expect("database definition");
        db.use_db("cleanup_schema").await.expect("schema database");
        db.query(
            "DEFINE TABLE cleanup_schema_row SCHEMAFULL;
             DEFINE FIELD payload ON cleanup_schema_row FLEXIBLE TYPE object;
             DEFINE FIELD revision ON cleanup_schema_row TYPE int DEFAULT 7;
             DEFINE TABLE cleanup_schema_audit SCHEMALESS;
             CREATE cleanup_schema_audit:row SET calls=0 RETURN NONE;
             DEFINE EVENT cleanup_after ON cleanup_schema_row
                 WHEN $event = 'CREATE' OR $event = 'UPDATE' THEN {
                     UPDATE cleanup_schema_audit:row SET calls+=1,
                         revision=$after.revision, payload=$after.payload RETURN NONE;
                 };
             CREATE cleanup_schema_row:row SET payload={retained: NULL} RETURN NONE;
             UPDATE cleanup_schema_row:row SET revision+=1 RETURN NONE;
             UPDATE cleanup_schema_row:row SET revision+=1,
                 payload={retained: NULL, absent: NONE} RETURN NONE;",
        )
        .await
        .expect("schema fixture response")
        .check()
        .expect("field defaults and event fixture");
        let expected_row = object([
            ("id", Thing::from(("cleanup_schema_row", "row")).into()),
            ("payload", object([("retained", Value::Null)])),
            ("revision", 9_i64.into()),
        ]);
        exact(
            &value(
                &db,
                "SELECT * FROM ONLY cleanup_schema_row:row;",
                BTreeMap::new(),
            )
            .await
            .expect("field default and removed NONE"),
            &expected_row,
        );
        let result = value(
            &db,
            "UPDATE cleanup_schema_row:row SET revision+=1, unknown=42 RETURN NONE;
             SELECT * FROM ONLY cleanup_schema_row:row;",
            BTreeMap::new(),
        )
        .await;
        if strict {
            let expected_field = Idiom::from(vec![Part::Field("unknown".into())]);
            assert!(matches!(
                result,
                Err(surrealdb::Error::Db(surrealdb::error::Db::FieldUndefined {
                    table,
                    field,
                })) if table == "cleanup_schema_row" && field == expected_field
            ));
        } else {
            exact(
                &result.expect("schemafull removes undefined field"),
                &object([
                    ("id", Thing::from(("cleanup_schema_row", "row")).into()),
                    ("payload", object([("retained", Value::Null)])),
                    ("revision", 10_i64.into()),
                ]),
            );
        }
        let final_revision = if strict { 9_i64 } else { 10_i64 };
        // A clean nonce-style update and a dirty update both run the event;
        // a strict schema rejection cannot advance the event's durable count.
        exact(
            &value(
                &db,
                "SELECT * FROM ONLY cleanup_schema_audit:row;",
                BTreeMap::new(),
            )
            .await
            .expect("event postimage and effect count"),
            &object([
                ("id", Thing::from(("cleanup_schema_audit", "row")).into()),
                ("calls", if strict { 3_i64 } else { 4_i64 }.into()),
                ("payload", object([("retained", Value::Null)])),
                ("revision", final_revision.into()),
            ]),
        );
        exact(
            &value(
                &db,
                "SELECT * FROM ONLY cleanup_schema_row:row;",
                BTreeMap::new(),
            )
            .await
            .expect("fresh schema postimage after rejected or cleaned write"),
            &object([
                ("id", Thing::from(("cleanup_schema_row", "row")).into()),
                ("payload", object([("retained", Value::Null)])),
                ("revision", final_revision.into()),
            ]),
        );
    }
}
