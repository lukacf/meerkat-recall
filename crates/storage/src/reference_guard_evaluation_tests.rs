//! Native reference-guard evaluation boundaries, including stored futures.
//! The frozen original function is a test oracle, never a runtime fallback.
#![allow(clippy::too_many_lines)]
use super::*;
use surrealdb::engine::local::{Db, Mem};
use surrealdb::Surreal;

const SPACE: &str = "guard-evaluation";

// A distinct private test name keeps this oracle on the original evaluator
// if the runtime later specializes the exact production function name/body.
fn guard_sql(original: bool, sql: &str) -> String {
    if original {
        sql.replace(
            "fn::recall_reference_guard",
            "fn::guard_evaluation_original",
        )
    } else {
        sql.to_owned()
    }
}

async fn native(original: bool) -> Surreal<Db> {
    let db = Surreal::new::<Mem>(()).await.expect("native database");
    db.use_ns("guard-evaluation")
        .use_db("guard-evaluation")
        .await
        .expect("original namespace");
    db.query(include_str!(
        "../../migrations/sql/039_private_reference_fence.surql"
    ))
    .await
    .expect("current reference schema response")
    .check()
    .expect("current reference schema");
    if original {
        db.query(guard_sql(
            true,
            include_str!("reference_guard_original.surql"),
        ))
        .await
        .expect("original function response")
        .check()
        .expect("frozen original function");
    }
    db
}

async fn counter(db: &Surreal<Db>) -> i64 {
    let mut response = db
        .query("SELECT VALUE count FROM ONLY guard_evaluation_counter:reads;")
        .await
        .expect("counter response")
        .check()
        .expect("counter read");
    response
        .take::<Option<i64>>(0)
        .expect("counter type")
        .expect("counter exists")
}

async fn revision(db: &Surreal<Db>) -> Option<i64> {
    let mut response = db
        .query("SELECT VALUE revision FROM ONLY $guard;")
        .bind(("guard", Thing::from((GUARD_TABLE, SPACE))))
        .await
        .expect("revision response")
        .check()
        .expect("revision read");
    response.take(0).expect("native revision type")
}

#[tokio::test]
async fn native_reference_guard_preserves_stored_future_effects_and_rollback() {
    for original in [true, false] {
        let db = native(original).await;
        db.query(
            "CREATE guard_evaluation_counter:reads SET count=0;\n\
             DEFINE FUNCTION fn::guard_evaluation_effect() {\n\
                 UPDATE guard_evaluation_counter:reads SET count+=1 RETURN NONE;\n\
                 RETURN 17;\n\
             };\n\
             CREATE $guard SET space_id=$space,journal_id=$journal,\
                 key_fingerprint=$fingerprint,version=1,membership_count=0,revision=0,\
                 unrelated=<future> { fn::guard_evaluation_effect() } RETURN NONE;",
        )
        .bind(("guard", Thing::from((GUARD_TABLE, SPACE))))
        .bind(("space", SPACE))
        .bind(("journal", JOURNAL))
        .bind((
            "fingerprint",
            reference_key_fingerprint(JOURNAL, &KEY).expect("fingerprint"),
        ))
        .await
        .expect("stored future fixture response")
        .check()
        .expect("stored future fixture");
        assert_eq!(
            counter(&db).await,
            0,
            "CREATE RETURN NONE must retain the future"
        );

        db.query(guard_sql(
            original,
            "BEGIN TRANSACTION;\n\
             fn::recall_reference_guard({space_id:$space},true);\n\
             COMMIT TRANSACTION;",
        ))
        .bind(("space", SPACE))
        .bind(("__recall_reference_key", Value::Bytes(KEY.to_vec().into())))
        .bind(("__recall_journal_identity", JOURNAL))
        .await
        .expect("original future-bearing guard response")
        .check()
        .expect("original future-bearing guard");
        // Both original SELECT * projections and the final UPDATE output
        // evaluate the stored future. A computed first row is not a raw row.
        assert_eq!(counter(&db).await, 3, "original={original}");
        assert_eq!(revision(&db).await, Some(1));

        let mut response = db
            .query(guard_sql(
                original,
                "BEGIN TRANSACTION;\n\
             fn::recall_reference_guard({space_id:$space},true);\n\
             THROW 'guard_evaluation_original_rollback';\n\
             COMMIT TRANSACTION;",
            ))
            .bind(("space", SPACE))
            .bind(("__recall_reference_key", Value::Bytes(KEY.to_vec().into())))
            .bind(("__recall_journal_identity", JOURNAL))
            .await
            .expect("rollback response");
        // The failed transaction rewrites the successful guard's result at
        // index 0; only index 1 proves that execution reached our later THROW.
        let errors = response.take_errors();
        assert_eq!(errors.len(), 2, "original={original}: {errors:?}");
        assert!(
            matches!(
                errors.get(&0),
                Some(surrealdb::Error::Db(surrealdb::error::Db::QueryNotExecuted))
            ),
            "original={original}: {errors:?}"
        );
        assert!(
            matches!(
                errors.get(&1),
                Some(surrealdb::Error::Db(surrealdb::error::Db::Thrown(message)))
                    if message == "guard_evaluation_original_rollback"
            ),
            "original={original}: {errors:?}"
        );
        assert_eq!(
            counter(&db).await,
            3,
            "all repeated future effects roll back"
        );
        assert_eq!(
            revision(&db).await,
            Some(1),
            "shared revision rolls back too"
        );
    }
}

#[tokio::test]
async fn native_reference_guard_observes_post_create_events_and_original_error_order() {
    for original in [true, false] {
        let db = native(original).await;
        db.query(
            "DEFINE EVENT OVERWRITE guard_evaluation_create ON TABLE source_erasure_guard\n\
             WHEN $event='CREATE' THEN {\n\
                 UPDATE $after.id SET erasure_reservation='original-owner',revision=-1 RETURN NONE;\n\
             };",
        )
        .await
        .expect("create event response")
        .check()
        .expect("create event");
        for (owner, expected) in [
            (None, "publication:erasure_space_reserved"),
            (
                Some("original-owner"),
                "publication:private_erasure_revision_exhausted",
            ),
        ] {
            let mut query = db
                .query(guard_sql(
                    original,
                    "BEGIN TRANSACTION;\n\
                 fn::recall_reference_guard({space_id:$space},true);\n\
                 COMMIT TRANSACTION;",
                ))
                .bind(("space", SPACE))
                .bind(("__recall_reference_key", Value::Bytes(KEY.to_vec().into())))
                .bind(("__recall_journal_identity", JOURNAL));
            if let Some(owner) = owner {
                query = query.bind(("__recall_erasure_reservation", owner));
            }
            let error = query
                .await
                .expect("post-create response")
                .check()
                .expect_err("actual post-create guard must refuse");
            assert!(
                error.to_string().contains(expected),
                "original={original}: {error}"
            );
            assert_eq!(
                revision(&db).await,
                None,
                "original CREATE and its event roll back"
            );
        }
        db.query(
            "DEFINE EVENT OVERWRITE guard_evaluation_create ON TABLE source_erasure_guard\n\
             WHEN $event='CREATE' THEN {\n\
                 UPDATE $after.id SET erasure_reservation='original-owner',revision=4 RETURN NONE;\n\
             };",
        )
        .await
        .expect("valid create event response")
        .check()
        .expect("valid create event");
        db.query(guard_sql(
            original,
            "BEGIN TRANSACTION;\n\
             fn::recall_reference_guard({space_id:$space},true);\n\
             COMMIT TRANSACTION;",
        ))
        .bind(("space", SPACE))
        .bind(("__recall_reference_key", Value::Bytes(KEY.to_vec().into())))
        .bind(("__recall_journal_identity", JOURNAL))
        .bind(("__recall_erasure_reservation", "original-owner"))
        .await
        .expect("owned post-create guard response")
        .check()
        .expect("owned post-create guard");
        assert_eq!(
            revision(&db).await,
            Some(5),
            "increment actual post-create revision"
        );
    }
}

#[tokio::test]
async fn native_reference_guard_validates_encoded_references_even_without_memberships() {
    let encoded = format!(
        "{}\"private_reference_fixture:42\"{}",
        "[".repeat(180),
        "]".repeat(180)
    );
    for original in [true, false] {
        let db = native(original).await;
        let response = db
            .query(guard_sql(
                original,
                "BEGIN TRANSACTION;\n\
             fn::recall_reference_guard({space_id:$space,encoded:$encoded},true);\n\
             COMMIT TRANSACTION;",
            ))
            .bind(("space", SPACE))
            .bind(("encoded", encoded.clone()))
            .bind(("__recall_reference_key", Value::Bytes(KEY.to_vec().into())))
            .bind(("__recall_journal_identity", JOURNAL))
            .await
            .expect("empty membership response");
        let error = response
            .check()
            .expect_err("full normalizer still runs without memberships");
        assert!(
            error
                .to_string()
                .contains("encoded reference nesting limit exceeded"),
            "original={original}: {error}"
        );
        assert_eq!(
            revision(&db).await,
            None,
            "failed normalization rolls back guard creation"
        );

        // The retained metadata-only path deliberately skips postimage
        // reference extraction while preserving custody and the shared write.
        db.query(guard_sql(
            original,
            "BEGIN TRANSACTION;\n\
             fn::recall_reference_guard({space_id:$space,encoded:$encoded},false);\n\
             COMMIT TRANSACTION;",
        ))
        .bind(("space", SPACE))
        .bind(("encoded", encoded.clone()))
        .bind(("__recall_reference_key", Value::Bytes(KEY.to_vec().into())))
        .bind(("__recall_journal_identity", JOURNAL))
        .await
        .expect("metadata-only response")
        .check()
        .expect("original metadata-only guard");
        assert_eq!(revision(&db).await, Some(1));
    }
}
