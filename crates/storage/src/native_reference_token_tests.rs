//! Fixed v1 vectors computed independently with Python stdlib HMAC-SHA256.
//! These tests exercise the public native token API and actual SQL function.
#![allow(clippy::expect_used)]

use std::collections::BTreeMap;
use surrealdb::dbs::Session;
use surrealdb::kvs::Datastore;
use surrealdb::sql::{Id, Thing, Value};

const JOURNAL: &str = "28a090cb-9741-43b0-8981-a9f030f6fc3c";
const KEY: [u8; 32] = [37; 32];
const UUID: &str = "8b12ceab-6f6a-4b12-8438-b31a56a3ddbb";

fn vectors() -> Vec<(Thing, &'static str)> {
    vec![
        (
            Thing::from(("source", "alpha")),
            "3740ce44a46e8af90318dc79ffd4c653a3b41206eb5fc8b25d6b285c969503fb",
        ),
        (
            Thing::from(("source", "beta")),
            "8c63d492a9e25562c1098f48836be20c3cc686e8dcde2d3f5575436eefb34de4",
        ),
        (
            Thing::from(("source", Id::Number(-7))),
            "bd6bc6f060219e105c17abf9a5b0f83546fe115dedfa012bf052b88de29c84c4",
        ),
        (
            Thing::from(("source", UUID)),
            "f24bf891016fce409a59137a471605ce8ac0f33170c2f91f3426137f3eaec0cc",
        ),
        (
            Thing::from(("extension", UUID)),
            "3d46d2df9de77ca5b9db0ee96b914074e3e16af1af41c366dbbc61a89a5e620a",
        ),
        (
            Thing::from((
                "edge",
                Id::Array(
                    vec![
                        Value::from("x"),
                        Value::Thing(Thing::from(("source", "alpha"))),
                        Value::from(7_i64),
                    ]
                    .into(),
                ),
            )),
            "23437d18d9e0d2ca6a3da15ed4f6ee80326b7d580b3f5c9bce1ed413617730ba",
        ),
        (
            Thing::from(("source", "-7")),
            "7cef11f64983d5811b1bbd3123fca6ef77c6aca7f039a51e61a007d8abf71234",
        ),
    ]
}

#[test]
fn native_reference_hmac_preserves_fixed_v1_identity_vectors_and_store_separation() {
    let tables = vec!["source".to_owned()];
    for (identity, expected) in vectors() {
        assert_eq!(
            surrealdb::recall_reference_identity_token(JOURNAL, "home", &KEY, &tables, &identity)
                .expect("fixed v1 identity"),
            expected
        );
        for (journal, space, key) in [
            (JOURNAL, "other-space", KEY),
            (JOURNAL, "home", [38; 32]),
            ("38a090cb-9741-43b0-8981-a9f030f6fc3c", "home", KEY),
        ] {
            assert_ne!(
                surrealdb::recall_reference_identity_token(
                    journal, space, &key, &tables, &identity
                )
                .expect("separate original store binding"),
                expected
            );
        }
    }
    let uuid = uuid::Uuid::parse_str(UUID).expect("fixed UUID");
    assert_eq!(
        surrealdb::recall_reference_identity_token(
            JOURNAL,
            "home",
            &KEY,
            &tables,
            &Thing::from(("source", Id::Uuid(uuid.into())))
        )
        .expect("native typed UUID"),
        vectors()[3].1
    );
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
async fn native_reference_hmac_sql_preserves_mixed_references_and_independent_calls() {
    let ds = Datastore::new("memory")
        .await
        .expect("actual native memory engine");
    let session = Session::owner()
        .with_ns("reference-hmac")
        .with_db("controls");
    let fixed = vectors();
    let mut values: Vec<Value> = fixed
        .iter()
        .map(|(identity, _)| Value::Thing(identity.clone()))
        .collect();
    // Duplicate native, canonical string, encoded JSON and object-key forms
    // still traverse normally and produce the exact original sorted token set.
    values.extend([
        Value::Thing(fixed[0].0.clone()),
        Value::from("source:beta"),
        Value::from("\"source:beta\""),
        Value::from("{\"tb\":\"source\",\"id\":\"alpha\"}"),
        Value::Object(
            BTreeMap::<String, Value>::from([("source:alpha".into(), Value::from("source:beta"))])
                .into(),
        ),
    ]);
    let mut expected: Vec<String> = fixed.iter().map(|(_, token)| (*token).to_owned()).collect();
    expected.sort();
    let query = "RETURN recall::reference_tokens($journal, $space, $key, $tables, $probe);";
    for value in [
        Value::Array(values.clone().into()),
        Value::Array(values.into_iter().rev().collect::<Vec<_>>().into()),
    ] {
        let bindings = BTreeMap::from([
            ("journal".into(), Value::from(JOURNAL)),
            ("space".into(), Value::from("home")),
            ("key".into(), Value::Bytes(KEY.to_vec().into())),
            (
                "tables".into(),
                Value::Array(vec![Value::from("source")].into()),
            ),
            ("probe".into(), value),
        ]);
        let mut responses = ds
            .execute(query, &session, Some(bindings))
            .await
            .expect("actual SQL token function");
        assert_eq!(responses.len(), 1);
        let actual = responses
            .remove(0)
            .result
            .expect("all references are valid native data");
        assert_eq!(
            actual,
            Value::Array(
                expected
                    .iter()
                    .map(|token| Value::from(token.clone()))
                    .collect::<Vec<_>>()
                    .into()
            )
        );
    }
}
