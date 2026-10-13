//! Native subquery parent snapshots retain values, not computed results.
#![allow(clippy::expect_used, clippy::too_many_lines)]

use surrealdb::engine::local::{Db, Mem};
use surrealdb::Surreal;

async fn native() -> Surreal<Db> {
    let db = Surreal::new::<Mem>(()).await.expect("native database");
    db.use_ns("parent-snapshot")
        .use_db("parent-snapshot")
        .await
        .expect("snapshot namespace");
    db
}

async fn integer(db: &Surreal<Db>, sql: &str) -> i64 {
    db.query(sql)
        .await
        .expect("integer query response")
        .check()
        .expect("integer query")
        .take::<Option<i64>>(0)
        .expect("integer type")
        .expect("integer row")
}

#[tokio::test]
async fn native_subquery_parent_retains_event_snapshot_across_nested_write() {
    let db = native().await;
    db.query("CREATE parent_snapshot:row SET marker=0 RETURN NONE;")
        .await
        .expect("fixture response")
        .check()
        .expect("fixture");

    // A normal SELECT starts with the immutable row loaded by Document::new.
    // Its nested subquery must still see that outer row as its parent.
    let nested: Vec<Vec<i64>> = db
        .query("SELECT VALUE (SELECT VALUE $parent.marker FROM [0]) FROM parent_snapshot:row;")
        .await
        .expect("ordinary parent response")
        .check()
        .expect("ordinary parent query")
        .take(0)
        .expect("nested ordinary parent values");
    assert_eq!(nested, vec![vec![0]]);

    // process_record_data calls to_mut before its sequential SET expressions.
    // The first assignment changes the in-memory cursor; the next subquery
    // captures that mutable value before the updated row is stored.
    db.query(
        "UPDATE parent_snapshot:row SET marker=7,
             captured=(SELECT VALUE $parent.marker FROM [0])[0] RETURN NONE;",
    )
    .await
    .expect("mutable parent response")
    .check()
    .expect("mutable parent update");
    assert_eq!(
        integer(&db, "SELECT VALUE captured FROM ONLY parent_snapshot:row;").await,
        7
    );
    db.query("UPDATE parent_snapshot:row SET marker=0 RETURN NONE;")
        .await
        .expect("reset before event response")
        .check()
        .expect("reset before event");

    db.query(
        "DEFINE EVENT parent_snapshot_event ON TABLE parent_snapshot
         WHEN $event='UPDATE' AND $after.marker=1 THEN {
             LET $first = (SELECT VALUE $parent.marker FROM [0])[0];
             UPDATE parent_snapshot:row SET marker=2 RETURN NONE;
             LET $second = (SELECT VALUE $parent.marker FROM [0])[0];
             CREATE parent_snapshot_audit:row SET first=$first,second=$second,
                 initial=$before.marker,after_marker=$after.marker,
                 live=(SELECT VALUE marker FROM ONLY parent_snapshot:row) RETURN NONE;
         };
         UPDATE parent_snapshot:row SET marker=1 RETURN NONE;",
    )
    .await
    .expect("nested event write response")
    .check()
    .expect("nested event write");

    // Event subqueries receive the immutable outer postimage, even after a
    // nested write changes the stored row. Before/after keep their own images.
    let actual: Vec<i64> = db
        .query(
            "SELECT VALUE [first,second,initial,after_marker,live] FROM ONLY parent_snapshot_audit:row;",
        )
        .await
        .expect("event observations response")
        .check()
        .expect("event observations")
        .take(0)
        .expect("event observation values");
    assert_eq!(actual, vec![1, 1, 0, 1, 2]);
    assert_eq!(
        integer(&db, "SELECT VALUE marker FROM ONLY parent_snapshot:row;").await,
        2
    );
}

#[tokio::test]
async fn native_subquery_parent_replays_stored_futures_and_rolls_back_effects() {
    let db = native().await;
    db.query(
        "CREATE parent_snapshot_counter:reads SET count=0 RETURN NONE;
         DEFINE FUNCTION fn::parent_snapshot_effect() {
             UPDATE parent_snapshot_counter:reads SET count+=1 RETURN NONE;
             RETURN (SELECT VALUE count FROM ONLY parent_snapshot_counter:reads);
         };
         CREATE parent_snapshot:future SET marker=0,
             deferred=<future> { fn::parent_snapshot_effect() } RETURN NONE;
         DEFINE EVENT parent_snapshot_future_event ON TABLE parent_snapshot
         WHEN $event='UPDATE' THEN {
             LET $first = (SELECT VALUE $parent.deferred FROM [0])[0];
             LET $second = (SELECT VALUE $parent.deferred FROM [0])[0];
             UPSERT parent_snapshot_audit:future SET first=$first,second=$second RETURN NONE;
         };",
    )
    .await
    .expect("stored future fixture response")
    .check()
    .expect("stored future fixture");
    let count = "SELECT VALUE count FROM ONLY parent_snapshot_counter:reads;";
    assert_eq!(integer(&db, count).await, 0);

    db.query("UPDATE parent_snapshot:future SET marker=1 RETURN NONE;")
        .await
        .expect("future event response")
        .check()
        .expect("future event");
    let actual: Vec<i64> = db
        .query("SELECT VALUE [first,second] FROM ONLY parent_snapshot_audit:future;")
        .await
        .expect("future values response")
        .check()
        .expect("future values")
        .take(0)
        .expect("future values type");
    assert_eq!(actual, vec![1, 2]);
    assert_eq!(integer(&db, count).await, 2);

    let mut response = db
        .query(
            "BEGIN TRANSACTION;
             UPDATE parent_snapshot:future SET marker=2 RETURN NONE;
             THROW 'parent_snapshot_rollback_after_effects';
             COMMIT TRANSACTION;",
        )
        .await
        .expect("rollback response");
    // Native rollback rewrites the earlier successful UPDATE result. Inspect
    // both indexed results so that an earlier event failure cannot stand in
    // for the deliberate THROW after those effects.
    let errors = response.take_errors();
    assert_eq!(errors.len(), 2, "{errors:?}");
    assert!(
        matches!(
            errors.get(&0),
            Some(surrealdb::Error::Db(surrealdb::error::Db::QueryNotExecuted))
        ),
        "{errors:?}"
    );
    assert!(
        matches!(
            errors.get(&1),
            Some(surrealdb::Error::Db(surrealdb::error::Db::Thrown(message)))
                if message == "parent_snapshot_rollback_after_effects"
        ),
        "{errors:?}"
    );
    assert_eq!(integer(&db, count).await, 2);
    // Projecting marker computes only that field, leaving deferred unevaluated.
    assert_eq!(
        integer(&db, "SELECT VALUE marker FROM ONLY parent_snapshot:future;").await,
        1
    );
    assert_eq!(integer(&db, count).await, 2);
}
