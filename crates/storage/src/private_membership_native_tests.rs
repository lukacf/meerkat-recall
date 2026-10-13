//! Executes the qualification helper's exact committed SQL on the actual native
//! SDK. This validates SQL and result shapes, not HTTP authentication or disk erasure.
#![allow(clippy::expect_used, clippy::panic, clippy::too_many_lines)]

use crate::reference_fence::{
    append_reference_membership_sql, reference_key_fingerprint, reference_tokens,
};
use crate::source_publication::stable_key;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use surrealdb::engine::local::{Db, Mem};
use surrealdb::sql::{Array, Id, Number, Object, Thing, Value};
use surrealdb::Surreal;

const HELPER: &str = include_str!("../../../scripts/qualification/native_private_membership.py");
const HELPER_SHA: &str = "59a95ca3ed8a2b6852438e8bf26120835ea90e91988dcb812e257b92d45a38a5";
const QUERY_SHA: &str = "8bfef45687997aed6a574288736e1750c21762e25fd46a50221e3cfe08f534d3";
const JOURNAL: &str = "5811e486-2a21-45cf-8f33-12c63397869a";
const KEY: [u8; 32] = [71; 32];
const TABLE: &str = "source_erasure_membership";

fn fixed_query() -> &'static str {
    // Full-file pin binds this test to L e9451c37444890e941b5054fa67a5d2b7819d4c3.
    // No copied SQL constant, Python execution, or alternate query is involved.
    assert_eq!(hex::encode(Sha256::digest(HELPER.as_bytes())), HELPER_SHA);
    let marker = "QUERY = \"\"\"";
    assert_eq!(HELPER.matches(marker).count(), 1);
    let (_, rest) = HELPER.split_once(marker).expect("fixed literal assignment");
    let (query, _) = rest.split_once("\"\"\"").expect("literal terminator");
    assert!(
        !query.contains('\\'),
        "literal extraction must not interpret escapes"
    );
    assert_eq!(hex::encode(Sha256::digest(query.as_bytes())), QUERY_SHA);
    query
}

async fn native() -> Surreal<Db> {
    let db = Surreal::new::<Mem>(())
        .await
        .expect("actual native SDK Mem");
    db.use_ns("membership-query-native")
        .use_db("membership-query-native")
        .await
        .expect("scope");
    // Actual retained migration, including its private table/index and events.
    // A mirrored test schema would not qualify the helper's INFO assumptions.
    db.query(include_str!(
        "../../migrations/sql/039_private_reference_fence.surql"
    ))
    .await
    .expect("production migration response")
    .check()
    .expect("production migration");
    db
}

async fn inspect(db: &Surreal<Db>) -> Vec<Value> {
    let mut response = db
        .query(fixed_query())
        .await
        .expect("fixed native query executes")
        .check()
        .expect("all five statements succeed");
    assert_eq!(response.num_statements(), 5);
    (0..5)
        .map(|index| {
            let value: surrealdb::Value = response.take(index).expect("lossless native result");
            value.into_inner()
        })
        .collect()
}

fn object(value: &Value) -> &Object {
    let Value::Object(value) = value else {
        panic!("native object required, got {value:?}");
    };
    value
}

fn array(value: &Value) -> &Array {
    let Value::Array(value) = value else {
        panic!("native array required, got {value:?}");
    };
    value
}

fn string(value: &Value) -> &str {
    let Value::Strand(value) = value else {
        panic!("native string required, got {value:?}");
    };
    &value.0
}

fn assert_schema(results: &[Value]) {
    let database = object(&results[0]);
    assert_eq!(
        database.keys().map(String::as_str).collect::<Vec<_>>(),
        [
            "accesses",
            "analyzers",
            "apis",
            "configs",
            "functions",
            "models",
            "params",
            "tables",
            "users"
        ]
    );
    for value in database.values() {
        object(value);
    }
    for name in ["accesses", "params", "users"] {
        assert!(object(&database[name]).is_empty());
    }
    assert_eq!(
        string(&object(&database["tables"])[TABLE]),
        "DEFINE TABLE source_erasure_membership TYPE ANY SCHEMALESS PERMISSIONS NONE"
    );
    let table = object(&results[1]);
    assert_eq!(
        table.keys().map(String::as_str).collect::<Vec<_>>(),
        ["events", "fields", "indexes", "lives", "tables"]
    );
    for name in ["events", "fields", "lives", "tables"] {
        assert!(object(&table[name]).is_empty());
    }
    let indexes = object(&table["indexes"]);
    assert_eq!(indexes.len(), 1);
    assert_eq!(string(&indexes["source_erasure_membership_space_token"]),
        "DEFINE INDEX source_erasure_membership_space_token ON source_erasure_membership FIELDS space_id, token UNIQUE");
}

fn count(value: &Value) -> i64 {
    let rows = array(value);
    if rows.is_empty() {
        return 0;
    }
    assert_eq!(rows.len(), 1);
    let row = object(&rows[0]);
    assert_eq!(row.len(), 1);
    let Value::Number(Number::Int(count)) = &row["count"] else {
        panic!("native integer count required");
    };
    assert!(*count >= 0);
    *count
}

async fn publish(db: &Surreal<Db>, space: &str, tokens: &BTreeSet<String>) {
    let mut sql = String::from("BEGIN TRANSACTION;\n");
    let mut bindings = BTreeMap::new();
    append_reference_membership_sql(
        &mut sql,
        &mut bindings,
        space,
        JOURNAL,
        &reference_key_fingerprint(JOURNAL, &KEY).expect("original key fingerprint"),
        tokens,
    )
    .expect("actual retained private projection writer");
    sql.push_str("COMMIT TRANSACTION;");
    db.query(sql)
        .bind(bindings)
        .await
        .expect("projection response")
        .check()
        .expect("projection commit");
}

fn tokens(space: &str, identity: &str) -> BTreeSet<String> {
    reference_tokens(
        JOURNAL,
        space,
        &KEY,
        &Value::Thing(Thing::from(("source", identity))),
    )
    .expect("actual original scoped reference normalizer")
}

#[tokio::test]
async fn native_private_membership_fixed_query_empty_production_schema_has_complete_native_shape() {
    let db = native().await;
    let results = inspect(&db).await;
    assert_schema(&results);
    assert_eq!(count(&results[2]), 0);
    assert!(array(&results[3]).is_empty());
    assert_eq!(count(&results[4]), 0);
    assert_eq!(inspect(&db).await, results, "inspection is read-only");
}

#[tokio::test]
async fn native_private_membership_fixed_query_populated_original_projection_preserves_native_witnesses(
) {
    let db = native().await;
    let first_space = "named-Å/研发";
    let first = tokens(first_space, "11111111-1111-4111-8111-111111111111");
    let second = tokens(first_space, "22222222-2222-4222-8222-222222222222");
    let other = tokens("named-b", "33333333-3333-4333-8333-333333333333");
    publish(&db, first_space, &first).await;
    publish(&db, first_space, &second).await;
    publish(&db, first_space, &first).await; // Original replay is a union, not duplicate rows.
    publish(&db, "named-b", &other).await;
    let expected = first
        .iter()
        .chain(&second)
        .map(|token| (first_space.to_owned(), token.clone()))
        .chain(
            other
                .iter()
                .map(|token| ("named-b".to_owned(), token.clone())),
        )
        .collect::<BTreeSet<_>>();
    assert_eq!(expected.len(), 3);
    let results = inspect(&db).await;
    assert_schema(&results);
    assert_eq!(count(&results[2]), 3);
    assert_eq!(count(&results[4]), 3);
    let rows = array(&results[3]);
    assert_eq!(rows.len(), 3);
    let mut actual = BTreeSet::new();
    for value in rows.iter() {
        let witness = object(value);
        assert_eq!(
            witness.keys().map(String::as_str).collect::<Vec<_>>(),
            [
                "id_inner",
                "id_is_record",
                "id_is_string",
                "keys",
                "row",
                "space_is_string",
                "token_is_string"
            ]
        );
        for field in [
            "id_is_record",
            "id_is_string",
            "space_is_string",
            "token_is_string",
        ] {
            assert_eq!(witness[field], Value::Bool(true), "{field}");
        }
        let row = object(&witness["row"]);
        assert_eq!(
            row.keys().map(String::as_str).collect::<Vec<_>>(),
            ["id", "space_id", "token"]
        );
        let keys = array(&witness["keys"])
            .iter()
            .map(string)
            .collect::<BTreeSet<_>>();
        assert_eq!(keys, BTreeSet::from(["id", "space_id", "token"]));
        assert_eq!(array(&witness["keys"]).len(), 3);
        let space = string(&row["space_id"]);
        let token = string(&row["token"]);
        let expected_id = stable_key(&[space, token]);
        let Value::Thing(id) = &row["id"] else {
            panic!("full native row must retain Thing identity");
        };
        assert_eq!(id.tb, TABLE);
        assert_eq!(id.id, Id::String(expected_id.clone()));
        assert_eq!(id.to_string(), format!("{TABLE}:`{expected_id}`"));
        assert_eq!(string(&witness["id_inner"]), expected_id);
        assert!(actual.insert((space.to_owned(), token.to_owned())));
    }
    assert_eq!(actual, expected);
    assert_eq!(
        inspect(&db).await,
        results,
        "inspection cannot mutate private state"
    );
}
