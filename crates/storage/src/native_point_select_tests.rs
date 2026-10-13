//! Native point reads and SET updates compared with retained general pipelines.
use super::{array, exact_native, native, object, query_value};
use std::collections::BTreeMap;
use surrealdb::sql::{Id, Thing, Value};

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
async fn native_point_select_preserves_native_values_missing_rows_and_only() {
    let client = native().await;
    let decimal = query_value(&client, "RETURN <decimal>7;", BTreeMap::new())
        .await
        .expect("actual native decimal");
    let uuid =
        uuid::Uuid::parse_str("247c0a2f-7e78-4b38-a206-77f09684472e").expect("fixed record UUID");
    let id = Value::Thing(Thing::from(("point_native", Id::Uuid(uuid.into()))));
    let row = object([
        ("id", id.clone()),
        ("integer", 7_i64.into()),
        ("float", 7.0_f64.into()),
        ("decimal", decimal),
        ("bytes", Value::Bytes(vec![0_u8, 255, 17].into())),
        ("uuid", Value::Uuid(uuid.into())),
        ("array", array([Value::None, Value::Null, "literal".into()])),
        ("nested", object([("literal.dot", true.into())])),
    ]);
    client
        .query_bind_checked(
            "CREATE $id CONTENT $row;",
            BTreeMap::<String, Value>::from([
                ("id".into(), id.clone()),
                ("row".into(), row.clone()),
            ]),
        )
        .await
        .expect("store native values without a JSON oracle");
    for (sql, expected) in [
        ("SELECT * FROM ONLY $id;", row.clone()),
        ("SELECT * FROM ONLY $id WHERE true;", row.clone()),
        ("SELECT * FROM $id;", array([row.clone()])),
        ("SELECT * FROM $id WHERE true;", array([row.clone()])),
        ("UPDATE ONLY $id SET integer=7;", row.clone()),
        ("UPDATE ONLY $id SET integer=7 RETURN AFTER;", row.clone()),
        ("UPDATE $id SET integer=7;", array([row.clone()])),
        (
            "UPDATE $id SET integer=7 RETURN AFTER;",
            array([row.clone()]),
        ),
        ("SELECT * FROM ONLY point_native:missing;", Value::None),
        (
            "SELECT * FROM ONLY point_native:missing WHERE true;",
            Value::None,
        ),
        ("SELECT * FROM point_native:missing;", array([])),
    ] {
        let actual = query_value(&client, sql, BTreeMap::from([("id".into(), id.clone())]))
            .await
            .expect(sql);
        exact_native(&actual, &expected);
    }
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
async fn native_point_select_keeps_target_projection_effects_and_transaction_rollback() {
    let client = native().await;
    client
        .query_checked(
            r"CREATE point_trace:order SET steps=[];
            CREATE point_effect:row SET nonce='original';
            DEFINE EVENT point_effect_event ON TABLE point_effect WHEN true THEN (
                UPDATE point_trace:order SET steps=array::append(steps, $event) RETURN NONE
            );
            DEFINE FUNCTION fn::point_target($label: string) {
                UPDATE point_trace:order SET steps=array::append(steps, $label) RETURN NONE;
                RETURN point_effect:row;
            };
            DEFINE FUNCTION fn::point_projection() {
                UPDATE point_trace:order SET steps=array::append(steps, 'projection') RETURN NONE;
                RETURN 19;
            };",
        )
        .await
        .expect("native target and projection effect fixtures");
    let actual = query_value(
        &client,
        r"BEGIN TRANSACTION;
        LET $first = SELECT * FROM ONLY fn::point_target('target-first');
        UPDATE point_effect:row SET nonce='updated' RETURN NONE;
        LET $second = SELECT * FROM ONLY fn::point_target('target-second');
        LET $projected = SELECT *, fn::point_projection() AS extra
            FROM ONLY fn::point_target('target-projected');
        RETURN [$first.nonce, $second.nonce, $projected.extra,
            (SELECT VALUE steps FROM ONLY point_trace:order)];
        COMMIT TRANSACTION;",
        BTreeMap::new(),
    )
    .await
    .expect("real target effects execute once, before document projection");
    let trace = array([
        "target-first".into(),
        "UPDATE".into(),
        "target-second".into(),
        "target-projected".into(),
        "projection".into(),
    ]);
    exact_native(
        &actual,
        &array([
            "original".into(),
            "updated".into(),
            19_i64.into(),
            trace.clone(),
        ]),
    );
    let failure = client
        .query_checked(
            r"BEGIN TRANSACTION;
            LET $before = SELECT * FROM ONLY fn::point_target('rolled-back-target');
            UPDATE point_effect:row SET nonce='rolled-back' RETURN NONE;
            THROW 'point_select_original_failure';
            COMMIT TRANSACTION;",
        )
        .await
        .expect_err("point reads cannot detach mutations or events from their transaction");
    assert!(
        failure
            .to_string()
            .contains("point_select_original_failure"),
        "{failure}"
    );
    let after = query_value(
        &client,
        "RETURN [(SELECT VALUE nonce FROM ONLY point_effect:row), (SELECT VALUE steps FROM ONLY point_trace:order)];",
        BTreeMap::new(),
    )
    .await
    .expect("failed target effects and events rolled back together");
    exact_native(&after, &array(["updated".into(), trace]));

    // Explicit AFTER retains the general iterator but has the same native
    // postimage computation as the default UPDATE output.
    for suffix in ["", " RETURN AFTER"] {
        client
            .query_checked(
                "UPDATE point_effect:row SET nonce='original' RETURN NONE; \
                 UPDATE point_trace:order SET steps=[] RETURN NONE;",
            )
            .await
            .expect("reset both paths to the same original state");
        let actual = query_value(
            &client,
            &format!(
                "BEGIN TRANSACTION; \
                 LET $written = UPDATE ONLY fn::point_target('update-target') \
                     SET nonce='written'{suffix}; \
                 RETURN [$written.nonce, (SELECT VALUE steps FROM ONLY point_trace:order)]; \
                 COMMIT TRANSACTION;"
            ),
            BTreeMap::new(),
        )
        .await
        .expect("target then original update event then postimage");
        let expected = array([
            "written".into(),
            array(["update-target".into(), "UPDATE".into()]),
        ]);
        exact_native(&actual, &expected);
        let error = client
            .query_checked(&format!(
                "BEGIN TRANSACTION; \
                 UPDATE ONLY fn::point_target('rollback-target') SET nonce='rollback'{suffix}; \
                 THROW 'point_update_original_failure'; COMMIT TRANSACTION;"
            ))
            .await
            .expect_err("point update target, row and event share rollback");
        assert!(
            error.to_string().contains("point_update_original_failure"),
            "{error}"
        );
        let after = query_value(
            &client,
            "RETURN [(SELECT VALUE nonce FROM ONLY point_effect:row), \
                (SELECT VALUE steps FROM ONLY point_trace:order)];",
            BTreeMap::new(),
        )
        .await
        .expect("original state survives the failed transaction");
        exact_native(&after, &expected);
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
async fn native_point_select_keeps_general_modifiers_targets_and_cardinality() {
    let client = native().await;
    client
        .query_checked("CREATE point_fallback:1 SET n=1; CREATE point_fallback:2 SET n=2;")
        .await
        .expect("two native fallback records");
    let one = object([
        (
            "id",
            Value::Thing(Thing::from(("point_fallback", Id::Number(1)))),
        ),
        ("n", 1_i64.into()),
    ]);
    let two = object([
        (
            "id",
            Value::Thing(Thing::from(("point_fallback", Id::Number(2)))),
        ),
        ("n", 2_i64.into()),
    ]);
    for (sql, expected) in [
        (
            "SELECT * FROM ONLY point_fallback:1 WITH NOINDEX;",
            one.clone(),
        ),
        ("SELECT * FROM ONLY point_fallback:1 LIMIT 1;", one.clone()),
        ("SELECT * FROM ONLY point_fallback:1 LIMIT 0;", Value::None),
        ("SELECT * FROM ONLY point_fallback:1 START 1;", Value::None),
        (
            "SELECT * FROM ONLY point_fallback:1 WHERE false;",
            Value::None,
        ),
        ("SELECT VALUE n FROM ONLY point_fallback:1;", 1_i64.into()),
        (
            "SELECT n FROM ONLY point_fallback:1;",
            object([("n", 1_i64.into())]),
        ),
        (
            "SELECT * OMIT id FROM ONLY point_fallback:1;",
            object([("n", 1_i64.into())]),
        ),
        (
            "SELECT * FROM [point_fallback:2, point_fallback:1] ORDER BY n;",
            array([one.clone(), two.clone()]),
        ),
        (
            "SELECT * FROM point_fallback:1, point_fallback:2 ORDER BY n DESC;",
            array([two.clone(), one.clone()]),
        ),
        (
            "SELECT * FROM point_fallback:1..=2 ORDER BY n;",
            array([one, two]),
        ),
    ] {
        let actual = query_value(&client, sql, BTreeMap::new()).await.expect(sql);
        exact_native(&actual, &expected);
    }
    for sql in [
        "SELECT * FROM ONLY point_fallback:1, point_fallback:2;",
        "SELECT * FROM ONLY [point_fallback:1, point_fallback:2];",
        "SELECT * FROM ONLY point_fallback:1..=2;",
    ] {
        let error = client.query_checked(sql).await.expect_err(sql);
        assert!(
            error.to_string().contains("single result"),
            "{sql}: {error}"
        );
    }
    for sql in [
        "UPDATE ONLY point_fallback:1 SET n=1;",
        "UPDATE ONLY point_fallback:1 WITH NOINDEX SET n=1;",
        "UPDATE ONLY point_fallback:1 SET n=1 WHERE true;",
        "UPDATE ONLY point_fallback:1 SET n=1 RETURN AFTER;",
        "UPDATE ONLY point_fallback:1 CONTENT {n:1};",
        "UPDATE ONLY point_fallback:1 MERGE {n:1};",
    ] {
        let actual = query_value(&client, sql, BTreeMap::new()).await.expect(sql);
        exact_native(
            &actual,
            &object([
                (
                    "id",
                    Value::Thing(Thing::from(("point_fallback", Id::Number(1)))),
                ),
                ("n", 1_i64.into()),
            ]),
        );
    }
    for sql in [
        "UPDATE point_fallback:1 SET n=99 WHERE false;",
        "UPDATE point_fallback:missing SET n=99;",
    ] {
        exact_native(
            &query_value(&client, sql, BTreeMap::new()).await.expect(sql),
            &array([]),
        );
    }
    // Bare arrays are not UPDATE target syntax. Preserve those parser refusals
    // separately, then use parenthesized values to reach target preparation.
    for sql in [
        "UPDATE ONLY [point_fallback:1] SET n=1;",
        "UPDATE ONLY [point_fallback:1, point_fallback:2] SET n=n;",
        "UPDATE [point_fallback:1] SET n=1;",
    ] {
        let error = client.query_checked(sql).await.expect_err(sql);
        assert!(error.to_string().contains("Parse error:"), "{sql}: {error}");
    }
    for sql in [
        "UPDATE ONLY point_fallback:missing SET n=99;",
        "UPDATE ONLY ([point_fallback:1]) SET n=1;",
        "UPDATE ONLY point_fallback:1, point_fallback:2 SET n=n;",
        "UPDATE ONLY ([point_fallback:1, point_fallback:2]) SET n=n;",
        "UPDATE ONLY point_fallback:1..=2 SET n=n;",
    ] {
        let error = client.query_checked(sql).await.expect_err(sql);
        assert!(
            error.to_string().contains("single result"),
            "{sql}: {error}"
        );
    }
    let array_target = query_value(
        &client,
        "UPDATE ([point_fallback:1]) SET n=1;",
        BTreeMap::new(),
    )
    .await
    .expect("a non-ONLY array target retains ordinary array output");
    exact_native(
        &array_target,
        &array([object([
            (
                "id",
                Value::Thing(Thing::from(("point_fallback", Id::Number(1)))),
            ),
            ("n", 1_i64.into()),
        ])]),
    );
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
async fn native_point_select_retains_original_depth_boundary_against_general_output() {
    let ds = Box::pin(surrealdb::kvs::Datastore::new("memory"))
        .await
        .expect("native depth datastore");
    let session = surrealdb::dbs::Session::owner()
        .with_ns("point_depth")
        .with_db("point_depth");
    let setup = Box::pin(ds.execute("CREATE point_depth:row SET n=7;", &session, None))
        .await
        .expect("native row creation");
    for response in setup {
        response.result.expect("native row created");
    }
    let maximum = usize::try_from(*surrealdb::cnf::MAX_COMPUTATION_DEPTH)
        .expect("original native computation budget");
    assert!((66..=256).contains(&maximum));
    // N enclosing array Values + one Subquery Value + the original iterator
    // dive(4) + one document Object Value + its scalar/Thing field Value.
    let boundary = maximum - 7;
    let row = object([
        ("id", Value::Thing(Thing::from(("point_depth", "row")))),
        ("n", 7_i64.into()),
    ]);
    for sql in [
        "(SELECT * FROM ONLY point_depth:row)",
        "(SELECT * FROM ONLY point_depth:row WITH NOINDEX)",
    ] {
        for levels in [0, boundary - 1, boundary, boundary + 1] {
            let mut input = surrealdb::sql::value(sql).expect("real native SELECT subquery AST");
            let mut expected = row.clone();
            for _ in 0..levels {
                input = array([input]);
                expected = array([expected]);
            }
            let actual = Box::pin(ds.compute(input, &session, None)).await;
            if levels <= boundary {
                exact_native(
                    &actual.expect("original remaining depth permits the row"),
                    &expected,
                );
            } else {
                let error = actual.expect_err("original iterator depth cost must still be charged");
                assert!(
                    matches!(error, surrealdb::error::Db::ComputationDepthExceeded),
                    "{sql}: {error}"
                );
            }
        }
    }

    // Compare all depths around the original write boundary. WITH NOINDEX
    // keeps the general UPDATE path without adding a condition evaluation.
    let mut saw_depth_error = false;
    for levels in std::iter::once(0).chain(maximum - 12..=maximum) {
        let mut results = Vec::new();
        let mut expected = row.clone();
        for _ in 0..levels {
            expected = array([expected]);
        }
        for sql in [
            "(UPDATE ONLY point_depth:row SET n=7)",
            "(UPDATE ONLY point_depth:row WITH NOINDEX SET n=7)",
        ] {
            let mut input = surrealdb::sql::value(sql).expect("native UPDATE subquery AST");
            for _ in 0..levels {
                input = array([input]);
            }
            results.push(Box::pin(ds.compute(input, &session, None)).await);
        }
        match (&results[0], &results[1]) {
            (Ok(point), Ok(general)) => {
                exact_native(point, &expected);
                exact_native(general, &expected);
            }
            (
                Err(surrealdb::error::Db::ComputationDepthExceeded),
                Err(surrealdb::error::Db::ComputationDepthExceeded),
            ) => {
                assert_ne!(levels, 0, "the unconstrained native update must succeed");
                saw_depth_error = true;
            }
            other => panic!("UPDATE depth {levels}: {other:?}"),
        }
    }
    assert!(
        saw_depth_error,
        "both original iterator depth boundaries were exercised"
    );
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
async fn native_subquery_dispatch_preserves_return_boundaries_and_parent_document() {
    let client = native().await;
    client
        .query_checked(
            "CREATE point_subquery_parent:one SET n=11; CREATE point_subquery_child:one SET parent=point_subquery_parent:one;",
        )
        .await
        .expect("native parent and child documents");
    // A Return error from the nested output belongs to that subquery. It must
    // not escape the surrounding array, terminate the query, or lose its doc.
    let actual = query_value(
        &client,
        r"LET $outer = 23;
        RETURN [
            (RETURN 17),
            (IF true THEN (RETURN $outer) ELSE (RETURN 0) END),
            (SELECT VALUE (RETURN $this.n) FROM ONLY point_subquery_parent:one),
            (SELECT VALUE (SELECT VALUE $parent.n FROM ONLY point_subquery_child:one)
                FROM ONLY point_subquery_parent:one),
            29
        ];",
        BTreeMap::new(),
    )
    .await
    .expect("nested Return boundaries retain outer context and parent documents");
    exact_native(
        &actual,
        &array([
            17_i64.into(),
            23_i64.into(),
            11_i64.into(),
            11_i64.into(),
            29_i64.into(),
        ]),
    );
}
