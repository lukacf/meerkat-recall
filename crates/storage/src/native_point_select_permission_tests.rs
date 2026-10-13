//! Native point reads and updates: permissions, errors and inherited planning.
use super::{array, exact_native, object};
use surrealdb::dbs::{capabilities::Capabilities, Session};
use surrealdb::error::Db;
use surrealdb::kvs::Datastore;
use surrealdb::sql::{Thing, Value};

const NS: &str = "point_select_controls";
const DB: &str = "point_select_controls";

fn owner() -> Session {
    Session::owner().with_ns(NS).with_db(DB)
}

async fn datastore(strict: bool) -> Datastore {
    let datastore = Box::pin(Datastore::new("memory"))
        .await
        .expect("actual native memory datastore")
        .with_auth_enabled(true)
        .with_strict_mode(strict)
        .with_capabilities(Capabilities::all());
    datastore.bootstrap().await.expect("native node lifecycle");
    datastore
}

async fn setup(datastore: &Datastore, sql: &str) {
    let responses = Box::pin(datastore.execute(sql, &owner(), None))
        .await
        .expect("parse original native setup");
    for response in responses {
        response.result.expect("original native setup succeeds");
    }
}

async fn statement(datastore: &Datastore, session: &Session, sql: &str) -> Result<Value, Db> {
    let mut responses = Box::pin(datastore.execute(sql, session, None))
        .await
        .expect("parse one original native statement");
    assert_eq!(responses.len(), 1, "{sql}");
    responses.remove(0).result
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
async fn native_point_select_rechecks_table_and_field_permissions() {
    let datastore = datastore(false).await;
    setup(
        &datastore,
        r"DEFINE ACCESS reader ON DATABASE TYPE RECORD;
        DEFINE TABLE point_reader SCHEMALESS PERMISSIONS FULL;
        CREATE point_reader:original;
        DEFINE TABLE point_permission SCHEMALESS
            PERMISSIONS FOR select WHERE allowed = true;
        DEFINE FIELD secret ON point_permission TYPE string PERMISSIONS FOR select NONE;
        CREATE point_permission:visible SET allowed=true, visible='visible', secret='hidden';
        CREATE point_permission:foreign SET allowed=false, visible='foreign', secret='foreign-hidden';
        DEFINE TABLE point_denied SCHEMALESS PERMISSIONS NONE;
        CREATE point_denied:original SET payload='owner-only';",
    )
    .await;
    let reader = Session::for_record(
        NS,
        DB,
        "reader",
        Value::Thing(Thing::from(("point_reader", "original"))),
    );
    let expected = object([
        (
            "id",
            Value::Thing(Thing::from(("point_permission", "visible"))),
        ),
        ("allowed", true.into()),
        ("visible", "visible".into()),
    ]);
    // WHERE true deliberately keeps the original general iterator. Both paths
    // must independently satisfy the literal permission and field oracle.
    for suffix in ["", " WHERE true"] {
        let visible = statement(
            &datastore,
            &reader,
            &format!("SELECT * FROM ONLY point_permission:visible{suffix};"),
        )
        .await
        .expect("allowed record with field redaction");
        exact_native(&visible, &expected);
        for id in ["point_permission:foreign", "point_denied:original"] {
            let refused = statement(
                &datastore,
                &reader,
                &format!("SELECT * FROM ONLY {id}{suffix};"),
            )
            .await
            .expect("denied native read is an empty selection");
            exact_native(&refused, &Value::None);
        }
    }
    let secret = statement(
        &datastore,
        &owner(),
        "SELECT VALUE secret FROM ONLY point_permission:visible;",
    )
    .await
    .expect("owner proves the hidden field is actually stored");
    exact_native(&secret, &Value::from("hidden"));

    setup(
        &datastore,
        "UPDATE point_permission:visible SET allowed=false RETURN NONE;",
    )
    .await;
    for suffix in ["", " WHERE true"] {
        let revoked = statement(
            &datastore,
            &reader,
            &format!("SELECT * FROM ONLY point_permission:visible{suffix};"),
        )
        .await
        .expect("current record permission is evaluated again");
        exact_native(&revoked, &Value::None);
    }
    setup(
        &datastore,
        r"DEFINE TABLE point_writer SCHEMALESS
            PERMISSIONS FOR select FULL FOR update WHERE allowed=true;
        DEFINE FIELD secret ON point_writer TYPE string
            PERMISSIONS FOR select, update NONE;
        CREATE point_writer:original SET allowed=true, n=0, secret='preserved';",
    )
    .await;
    for suffix in ["", " RETURN AFTER"] {
        let written = statement(
            &datastore,
            &reader,
            &format!("UPDATE ONLY point_writer:original SET n=7, secret='forbidden'{suffix};"),
        )
        .await
        .expect("allowed update keeps field permissions and postimage redaction");
        exact_native(
            &written,
            &object([
                (
                    "id",
                    Value::Thing(Thing::from(("point_writer", "original"))),
                ),
                ("allowed", true.into()),
                ("n", 7_i64.into()),
            ]),
        );
        exact_native(
            &statement(
                &datastore,
                &owner(),
                "SELECT VALUE secret FROM ONLY point_writer:original;",
            )
            .await
            .expect("owner checks the stored field"),
            &Value::from("preserved"),
        );
        let denied = statement(
            &datastore,
            &reader,
            &format!("UPDATE point_denied:original SET payload='forbidden'{suffix};"),
        )
        .await
        .expect("table-level refusal");
        exact_native(&denied, &array([]));
    }
    setup(
        &datastore,
        "UPDATE point_writer:original SET allowed=false RETURN NONE;",
    )
    .await;
    for suffix in ["", " RETURN AFTER"] {
        let denied = statement(
            &datastore,
            &reader,
            &format!("UPDATE point_writer:original SET n=99{suffix};"),
        )
        .await
        .expect("record update permission is rechecked after revocation");
        exact_native(&denied, &array([]));
    }
    exact_native(
        &statement(
            &datastore,
            &owner(),
            "SELECT VALUE n FROM ONLY point_writer:original;",
        )
        .await
        .expect("revoked updates cannot change the row"),
        &Value::from(7_i64),
    );
    datastore.shutdown().await.expect("native shutdown");
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
async fn native_point_select_missing_rows_keep_strict_errors_and_cardinality() {
    let datastore = datastore(true).await;
    setup(
        &datastore,
        r"DEFINE NAMESPACE point_select_controls;
        DEFINE DATABASE point_select_controls;
        DEFINE TABLE point_missing SCHEMALESS;",
    )
    .await;
    for suffix in ["", " WHERE true"] {
        let missing = statement(
            &datastore,
            &owner(),
            &format!("SELECT * FROM ONLY point_missing:absent{suffix};"),
        )
        .await
        .expect("missing record in an existing table");
        exact_native(&missing, &Value::None);
        let ordinary = statement(
            &datastore,
            &owner(),
            &format!("SELECT * FROM point_missing:absent{suffix};"),
        )
        .await
        .expect("ordinary missing selection remains an array");
        exact_native(&ordinary, &array([]));
        let missing_table = statement(
            &datastore,
            &owner(),
            &format!("SELECT * FROM ONLY point_undefined:absent{suffix};"),
        )
        .await;
        assert!(
            matches!(&missing_table, Err(Db::TbNotFound { name }) if name == "point_undefined"),
            "{missing_table:?}"
        );
    }
    for suffix in ["", " RETURN AFTER"] {
        let missing = statement(
            &datastore,
            &owner(),
            &format!("UPDATE point_missing:absent SET n=7{suffix};"),
        )
        .await
        .expect("UPDATE never creates the missing record");
        exact_native(&missing, &array([]));
        let only = statement(
            &datastore,
            &owner(),
            &format!("UPDATE ONLY point_missing:absent SET n=7{suffix};"),
        )
        .await;
        assert!(matches!(only, Err(Db::SingleOnlyOutput)), "{only:?}");
        let undefined = statement(
            &datastore,
            &owner(),
            &format!("UPDATE point_undefined:absent SET n=7{suffix};"),
        )
        .await;
        assert!(
            matches!(&undefined, Err(Db::TbNotFound { name }) if name == "point_undefined"),
            "{undefined:?}"
        );
        // A bare number is a syntax error; a parenthesized number reaches the
        // original preparation error for an unsupported computed target.
        let parse_error =
            Box::pin(datastore.execute(&format!("UPDATE 17 SET n=7{suffix};"), &owner(), None))
                .await
                .expect_err("bare numeric UPDATE target is not valid syntax");
        assert!(
            matches!(&parse_error, Db::InvalidQuery(_)),
            "{parse_error:?}"
        );
        let invalid = statement(
            &datastore,
            &owner(),
            &format!("UPDATE (17) SET n=7{suffix};"),
        )
        .await;
        assert!(
            matches!(invalid, Err(Db::UpdateStatement { .. })),
            "{invalid:?}"
        );
    }
    datastore.shutdown().await.expect("native shutdown");
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
async fn native_point_select_keeps_stored_futures_original_errors_and_timeout() {
    let datastore = datastore(false).await;
    setup(
        &datastore,
        r"CREATE point_future:original SET answer=<future> { 6 * 7 } RETURN NONE;
        CREATE point_future:failure SET answer=<future> {
            THROW 'point_select_original_failure';
        } RETURN NONE;",
    )
    .await;
    let expected = object([
        (
            "id",
            Value::Thing(Thing::from(("point_future", "original"))),
        ),
        ("answer", 42_i64.into()),
    ]);
    for suffix in ["", " WHERE true"] {
        let actual = statement(
            &datastore,
            &owner(),
            &format!("SELECT * FROM ONLY point_future:original{suffix};"),
        )
        .await
        .expect("SELECT star evaluates the stored future");
        exact_native(&actual, &expected);
        let failure = statement(
            &datastore,
            &owner(),
            &format!("SELECT * FROM ONLY point_future:failure{suffix};"),
        )
        .await;
        assert!(
            matches!(&failure, Err(Db::Thrown(message)) if message == "point_select_original_failure"),
            "{failure:?}"
        );
    }
    // TIMEOUT intentionally remains a general-path clause. Zero duration is an
    // existing native timeout oracle and needs no sleep or timing tolerance.
    let timeout = statement(
        &datastore,
        &owner(),
        "SELECT * FROM ONLY point_future:original TIMEOUT 0s;",
    )
    .await;
    assert!(matches!(timeout, Err(Db::QueryTimedout)), "{timeout:?}");
    let after = statement(
        &datastore,
        &owner(),
        "SELECT * FROM ONLY point_future:original;",
    )
    .await
    .expect("failed and timed out reads do not corrupt the next read");
    exact_native(&after, &expected);
    setup(
        &datastore,
        r"CREATE point_update_trace:original SET steps=[];
        DEFINE FUNCTION fn::point_update_future() {
            UPDATE point_update_trace:original SET steps=array::append(steps, 'future') RETURN NONE;
            RETURN 42;
        };
        CREATE point_update_future:original SET n=0,
            answer=<future> { fn::point_update_future() } RETURN NONE;
        DEFINE EVENT point_update_event ON TABLE point_update_future WHEN true THEN (
            UPDATE point_update_trace:original SET steps=array::append(steps, 'event') RETURN NONE
        );",
    )
    .await;
    for suffix in ["", " RETURN AFTER"] {
        setup(
            &datastore,
            "UPDATE point_update_future:original SET n=0 RETURN NONE; \
             UPDATE point_update_trace:original SET steps=[] RETURN NONE;",
        )
        .await;
        let written = statement(
            &datastore,
            &owner(),
            &format!("UPDATE ONLY point_update_future:original SET n=7{suffix};"),
        )
        .await
        .expect("default postimage computes the stored future after the event");
        exact_native(
            &written,
            &object([
                (
                    "id",
                    Value::Thing(Thing::from(("point_update_future", "original"))),
                ),
                ("n", 7_i64.into()),
                ("answer", 42_i64.into()),
            ]),
        );
        exact_native(
            &statement(
                &datastore,
                &owner(),
                "SELECT VALUE steps FROM ONLY point_update_trace:original;",
            )
            .await
            .expect("observe effects without evaluating the future again"),
            &array(["event".into(), "future".into()]),
        );
        let failure = statement(
            &datastore,
            &owner(),
            &format!("UPDATE ONLY point_future:failure SET n=99{suffix};"),
        )
        .await;
        assert!(
            matches!(&failure, Err(Db::Thrown(message)) if message == "point_select_original_failure"),
            "{failure:?}"
        );
        exact_native(
            &statement(
                &datastore,
                &owner(),
                "SELECT VALUE n FROM ONLY point_future:failure;",
            )
            .await
            .expect("failed postimage rolls the mutation back"),
            &Value::None,
        );
    }
    let timeout = statement(
        &datastore,
        &owner(),
        "UPDATE ONLY point_update_future:original SET n=99 TIMEOUT 0s;",
    )
    .await;
    assert!(matches!(timeout, Err(Db::QueryTimedout)), "{timeout:?}");
    exact_native(
        &statement(
            &datastore,
            &owner(),
            "SELECT VALUE n FROM ONLY point_update_future:original;",
        )
        .await
        .expect("timeout preserves the original row"),
        &Value::from(7_i64),
    );
    datastore.shutdown().await.expect("native shutdown");
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
async fn native_point_select_nested_in_actual_index_plan_keeps_original_results() {
    let datastore = datastore(false).await;
    // Complete the definition before writes, so the test is about current
    // indexed planning, not concurrent index construction over existing rows.
    setup(
        &datastore,
        "DEFINE INDEX point_parent_marker ON point_parent FIELDS marker;",
    )
    .await;
    setup(
        &datastore,
        r"CREATE point_lookup:original SET value=17;
        CREATE point_parent:chosen SET marker='keep';
        CREATE point_parent:distractor SET marker='drop';",
    )
    .await;
    let plan = statement(
        &datastore,
        &owner(),
        "SELECT * FROM point_parent WITH INDEX point_parent_marker WHERE marker='keep' EXPLAIN;",
    )
    .await
    .expect("actual indexed outer plan");
    let Value::Array(steps) = plan else {
        panic!("native EXPLAIN must return an array");
    };
    assert!(steps.iter().any(|step| {
        matches!(step, Value::Object(fields)
            if fields.get("operation") == Some(&Value::from("Iterate Index")))
    }));
    for value in [17_i64, 23] {
        setup(
            &datastore,
            &format!("UPDATE point_lookup:original SET value={value} RETURN NONE;"),
        )
        .await;
        let expected = array([object([(
            "nested",
            object([
                (
                    "id",
                    Value::Thing(Thing::from(("point_lookup", "original"))),
                ),
                ("value", value.into()),
            ]),
        )])]);
        for suffix in ["", " WHERE true"] {
            let actual = statement(
                &datastore,
                &owner(),
                &format!(
                    "SELECT (SELECT * FROM ONLY point_lookup:original{suffix}) AS nested \
                     FROM point_parent WITH INDEX point_parent_marker WHERE marker='keep';"
                ),
            )
            .await
            .expect("nested point query under the actual inherited index planner");
            exact_native(&actual, &expected);
        }
        for suffix in ["", " RETURN AFTER"] {
            let actual = statement(
                &datastore,
                &owner(),
                &format!(
                "SELECT (UPDATE ONLY point_lookup:original SET value={value}{suffix}) AS nested \
                 FROM point_parent WITH INDEX point_parent_marker WHERE marker='keep';"
            ),
            )
            .await
            .expect("nested update retains the inherited index planner");
            exact_native(&actual, &expected);
        }
    }
    datastore.shutdown().await.expect("native shutdown");
}
