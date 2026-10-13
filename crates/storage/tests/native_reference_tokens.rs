//! Fixed-vector and actual SDK/event coverage for the pinned Recall capability.
use std::collections::BTreeMap;
use surrealdb::engine::local::Mem;
use surrealdb::sql::{Id, Object, Thing, Value};
use surrealdb::{recall_reference_tokens, Surreal, RECALL_REFERENCE_TOKEN_VERSION};

const JOURNAL: &str = "00000000-0000-0000-0000-000000000001";
const SPACE: &str = "__recall_capability__";
const STRING_TOKEN: &str = "40e9322203b5e4d741f086599f851ac1b7c4e8a2e0bad1398da4ffde5911874c";
const NUMBER_TOKEN: &str = "03d2d2962853eaa5410909793d65f296968ae5e6024ab522121d575cf1108454";

#[expect(
    clippy::unwrap_used,
    reason = "Retain the original fixed-vector key construction and fail the fixture if its bounded byte conversion fails."
)]
fn key() -> [u8; 32] {
    std::array::from_fn(|index| u8::try_from(index).unwrap())
}

fn reference(table: &str, id: Id) -> Value {
    Value::Thing(Thing::from((table, id)))
}

fn object(entries: impl IntoIterator<Item = (&'static str, Value)>) -> Value {
    Value::Object(Object::from(
        entries
            .into_iter()
            .map(|(key, value)| (key.to_owned(), value))
            .collect::<BTreeMap<_, _>>(),
    ))
}

#[expect(
    clippy::unwrap_used,
    reason = "Retain the original native-token oracle call and immediate failure if a fixed fixture cannot be tokenized."
)]
fn tokens(value: &Value) -> Vec<String> {
    recall_reference_tokens(JOURNAL, SPACE, &key(), &[], value).unwrap()
}

#[test]
fn native_reference_capability_matches_independent_fixed_vectors() {
    assert_eq!(RECALL_REFERENCE_TOKEN_VERSION, 1);
    let string = reference("recall_probe", Id::String("42".into()));
    let number = reference("recall_probe", Id::Number(42));
    assert_eq!(tokens(&string), [STRING_TOKEN]);
    assert_eq!(tokens(&number), [NUMBER_TOKEN]);
    assert_eq!(
        tokens(&Value::from(vec![string.clone(), number, string])),
        [NUMBER_TOKEN, STRING_TOKEN]
    );
}

#[test]
#[expect(
    clippy::unwrap_used,
    reason = "Retain exact encoded fixture construction and all original normalization equality assertions."
)]
fn native_reference_normalizes_exact_encoded_and_object_forms() {
    let string = reference("recall_probe", Id::String("42".into()));
    let native_string = string.to_string();
    let json = serde_json::json!({
        "outer": serde_json::to_string(&serde_json::json!({"tb": "recall_probe", "id": "42"})).unwrap(),
        (native_string.clone()): {"table": "recall_probe", "id": 42},
    });
    assert_eq!(
        tokens(&Value::from(serde_json::to_string(&json).unwrap())),
        [NUMBER_TOKEN, STRING_TOKEN]
    );
    assert_eq!(
        tokens(&object([
            ("tb", "recall_probe".into()),
            ("id", "42".into())
        ])),
        [STRING_TOKEN]
    );
    assert_eq!(
        tokens(&object([
            ("table", "recall_probe".into()),
            ("id", 42_i64.into())
        ])),
        [NUMBER_TOKEN]
    );
    assert_eq!(tokens(&Value::from(native_string)), [STRING_TOKEN]);
    assert_eq!(tokens(&Value::from("recall_probe:42")), [NUMBER_TOKEN]);
}

#[test]
fn native_reference_does_not_search_prose_or_bare_uuid() {
    for value in [
        "ordinary prose recall_probe:42 ending",
        "recall_probe:42 /* ordinary prose */",
        "recall_probe:42 suffix",
        "recall_probe:42/trailing",
        "00000000-0000-0000-0000-000000000001",
    ] {
        assert!(
            tokens(&Value::from(value)).is_empty(),
            "unexpected exact reference in {value:?}"
        );
    }
    assert_ne!(
        tokens(&reference("recall_probe", Id::Number(420))),
        [NUMBER_TOKEN]
    );
    assert_ne!(
        tokens(&reference("another_table", Id::Number(42))),
        [NUMBER_TOKEN]
    );
}

#[test]
#[expect(
    clippy::unwrap_used,
    reason = "Retain the original UUID fixtures and table-specific normalization assertions without changing error boundaries."
)]
fn native_reference_uuid_normalization_is_table_specific() {
    let id = uuid::Uuid::parse_str(JOURNAL).unwrap();
    let string = reference("source", Id::String(JOURNAL.into()));
    let native = reference("source", Id::Uuid(id.into()));
    let typed = vec!["source".to_owned()];
    assert_eq!(
        recall_reference_tokens(JOURNAL, SPACE, &key(), &typed, &string).unwrap(),
        recall_reference_tokens(JOURNAL, SPACE, &key(), &typed, &native).unwrap(),
    );
    assert_ne!(tokens(&string), tokens(&native));
    assert_ne!(
        recall_reference_tokens(
            JOURNAL,
            SPACE,
            &key(),
            &typed,
            &reference("extension", Id::String(JOURNAL.into()))
        )
        .unwrap(),
        recall_reference_tokens(
            JOURNAL,
            SPACE,
            &key(),
            &typed,
            &reference("extension", Id::Uuid(id.into()))
        )
        .unwrap(),
    );
}

#[test]
fn native_reference_composite_identity_preserves_shape_and_nested_references() {
    let nested = reference("recall_probe", Id::Number(42));
    let array = reference(
        "extension",
        Id::Array(vec![Value::from("row"), nested].into()),
    );
    let object_id = reference(
        "extension",
        Id::Object(Object::from(BTreeMap::from([(
            "row".to_owned(),
            Value::from(42_i64),
        )]))),
    );
    let values = tokens(&array);
    assert_eq!(values.len(), 2);
    assert!(values.contains(&NUMBER_TOKEN.to_owned()));
    assert_ne!(values, tokens(&object_id));
    let encoded = Value::from(array.to_string());
    assert_eq!(tokens(&encoded), values);
    let large_one = Value::from(r#"{"tb":"extension","id":[18446744073709551614]}"#);
    let large_two = Value::from(r#"{"tb":"extension","id":[18446744073709551615]}"#);
    assert_ne!(tokens(&large_one), tokens(&large_two));
}

#[test]
#[expect(
    clippy::unwrap_used,
    reason = "Retain the original successful token fixtures and separate journal, space and private-key binding assertions."
)]
fn native_reference_tokens_bind_journal_space_and_private_key() {
    let value = reference("recall_probe", Id::Number(42));
    let expected = tokens(&value);
    assert_ne!(
        expected,
        recall_reference_tokens(
            "00000000-0000-0000-0000-000000000002",
            SPACE,
            &key(),
            &[],
            &value
        )
        .unwrap()
    );
    assert_ne!(
        expected,
        recall_reference_tokens(JOURNAL, "other-space", &key(), &[], &value).unwrap()
    );
    assert_ne!(
        expected,
        recall_reference_tokens(JOURNAL, SPACE, &[42; 32], &[], &value).unwrap()
    );
    assert!(expected.iter().all(|token| token.len() == 64
        && token
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))));
}

#[tokio::test]
#[expect(
    clippy::cloned_ref_to_slice_refs,
    clippy::needless_raw_string_hashes,
    clippy::unwrap_used,
    reason = "Retain exact raw SQL spelling, owned expected-token clone and original SDK, closure, event and missing-key assertions."
)]
async fn native_reference_bound_context_survives_custom_function_closure_and_event() {
    let db = Surreal::new::<Mem>(()).await.unwrap();
    db.use_ns("recall")
        .use_db("native-reference")
        .await
        .unwrap();
    let expected = vec![NUMBER_TOKEN.to_owned()];
    let query = r#"
        DEFINE FUNCTION fn::recall_probe($value: any) {
            RETURN recall::reference_tokens($__recall_journal_identity, '__recall_capability__', $__recall_reference_key, [], $value);
        };
        DEFINE TABLE recall_write SCHEMALESS;
        DEFINE TABLE recall_audit SCHEMALESS;
        DEFINE EVENT recall_probe ON recall_write WHEN $event = 'CREATE' OR $event = 'UPDATE' THEN {
            CREATE recall_audit:only SET tokens = fn::recall_probe($after.reference);
        };
        RETURN recall::reference_token_version();
        RETURN fn::recall_probe(recall_probe:42);
        RETURN array::map([recall_probe:42], |$reference_value| $reference_value);
        RETURN array::map([recall_probe:42], |$reference_value| fn::recall_probe($reference_value));
        CREATE recall_write:only SET reference = recall_probe:42;
        RETURN (SELECT VALUE tokens FROM ONLY recall_audit:only);
    "#;
    let mut response = db
        .query(query)
        .bind(("__recall_journal_identity", JOURNAL))
        .bind((
            "__recall_reference_key",
            Value::Bytes(key().to_vec().into()),
        ))
        .await
        .unwrap()
        .check()
        .unwrap();
    assert_eq!(response.take::<Option<i64>>(4).unwrap(), Some(1));
    assert_eq!(response.take::<Vec<String>>(5).unwrap(), expected);
    // In the pinned engine, closure argument binding formats identifiers while
    // parameter lookup uses their raw name. Keep the non-reserved argument name
    // above and prove the actual Thing arrives before testing nested custody.
    assert_eq!(
        response.take::<Vec<Thing>>(6).unwrap(),
        [Thing::from(("recall_probe", Id::Number(42)))]
    );
    assert_eq!(
        response.take::<Vec<Vec<String>>>(7).unwrap(),
        [expected.clone()]
    );
    assert_eq!(response.take::<Vec<String>>(9).unwrap(), expected);

    // A fresh query does not retain query-private bindings. The event must
    // reject the write instead of silently producing an empty membership.
    assert!(db
        .query("CREATE recall_write:without_key SET reference = recall_probe:42;")
        .await
        .unwrap()
        .check()
        .is_err());
    let mut response = db
        .query("RETURN (SELECT VALUE id FROM recall_write);")
        .await
        .unwrap()
        .check()
        .unwrap();
    assert_eq!(
        response.take::<Vec<Thing>>(0).unwrap(),
        [Thing::from(("recall_write", "only"))]
    );
}

#[tokio::test]
#[expect(
    clippy::unwrap_used,
    reason = "Retain the original SDK setup and exact failure boundary used to check argument-error redaction."
)]
async fn native_reference_argument_errors_do_not_disclose_supplied_values() {
    let db = Surreal::new::<Mem>(()).await.unwrap();
    db.use_ns("recall").use_db("argument-errors").await.unwrap();
    let secret = "nonsecret-test-marker-that-must-not-appear";
    let error = db
        .query("RETURN recall::reference_tokens($journal, $space, $key, [], $value);")
        .bind(("journal", JOURNAL))
        .bind(("space", SPACE))
        .bind(("key", secret))
        .bind((
            "value",
            reference("private_table", Id::String(secret.into())),
        ))
        .await
        .unwrap()
        .check()
        .unwrap_err()
        .to_string();
    assert!(!error.contains(secret));
    assert!(!error.contains("private_table"));
    assert!(error.contains("invalid reference token argument types"));
}

#[tokio::test]
#[expect(
    clippy::unwrap_used,
    reason = "Retain the original direct and SDK token calls and exact owned-table parity assertions."
)]
async fn native_reference_owned_table_arguments_preserve_public_token_parity() {
    let db = Surreal::new::<Mem>(()).await.unwrap();
    db.use_ns("recall")
        .use_db("owned-table-parity")
        .await
        .unwrap();
    let uuid = uuid::Uuid::parse_str(JOURNAL).unwrap();
    let value = Value::from(vec![
        reference("source", Id::String(JOURNAL.into())),
        reference("source", Id::Uuid(uuid.into())),
        reference("extension", Id::String(JOURNAL.into())),
        reference("extension", Id::Uuid(uuid.into())),
        reference("recall_probe", Id::Number(42)),
        reference("recall_probe", Id::String("42".into())),
        reference("recall_probe", Id::Number(42)),
        Value::from(r#"{"tb":"recall_probe","id":42}"#),
    ]);
    let configurations = [
        Vec::<String>::new(),
        vec!["unused_table".into(), "source".into(), "source".into()],
        vec!["source".into()],
        vec!["extension".into(), "source".into(), "extension".into()],
        Vec::new(),
    ];
    for tables in configurations {
        let expected = recall_reference_tokens(JOURNAL, SPACE, &key(), &tables, &value).unwrap();
        let mut response = db
            .query("RETURN recall::reference_tokens($journal, $space, $key, $tables, $value);")
            .bind(("journal", JOURNAL))
            .bind(("space", SPACE))
            .bind(("key", Value::Bytes(key().to_vec().into())))
            .bind(("tables", tables))
            .bind(("value", value.clone()))
            .await
            .unwrap()
            .check()
            .unwrap();
        let actual = response.take::<Vec<String>>(0).unwrap();
        assert_eq!(actual, expected);
        assert!(actual.windows(2).all(|pair| pair[0] < pair[1]));
        assert!(actual.contains(&STRING_TOKEN.to_owned()));
        assert!(actual.contains(&NUMBER_TOKEN.to_owned()));
    }
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    clippy::unwrap_used,
    reason = "Retain the complete original argument-precedence and redaction case matrix, with each original success and refusal boundary."
)]
async fn native_reference_owned_table_arguments_preserve_error_precedence_and_redaction() {
    let db = Surreal::new::<Mem>(()).await.unwrap();
    db.use_ns("recall")
        .use_db("owned-table-errors")
        .await
        .unwrap();
    let private = "private-table-argument-sentinel";
    let valid_key = Value::Bytes(vec![37_u8; 32].into());
    let short_key = Value::Bytes(vec![37_u8; 31].into());
    let invalid_names = Value::from(vec![Value::from(private)]);
    let mixed_tables = Value::from(vec![Value::from(private), Value::Bool(false)]);
    let valid_tables = Value::from(vec![Value::from("source")]);
    let query = "RETURN recall::reference_tokens($journal, $space, $key, $tables, $value);";
    let cases = [
        (
            "RETURN recall::reference_tokens($journal, $space, $key, $tables);",
            Value::None,
            Value::None,
            short_key.clone(),
            mixed_tables.clone(),
            "expected journal, space, private bytes, typed tables and value",
        ),
        (
            query,
            Value::None,
            Value::from(SPACE),
            short_key.clone(),
            mixed_tables.clone(),
            "invalid reference token argument types",
        ),
        (
            query,
            Value::from(JOURNAL),
            Value::from(SPACE),
            Value::from(private),
            mixed_tables.clone(),
            "invalid reference token argument types",
        ),
        (
            query,
            Value::from(JOURNAL),
            Value::from(SPACE),
            short_key.clone(),
            Value::None,
            "invalid reference token argument types",
        ),
        (
            query,
            Value::from(private),
            Value::from(""),
            short_key,
            mixed_tables.clone(),
            "invalid reference key length",
        ),
        (
            query,
            Value::from(private),
            Value::from(""),
            valid_key.clone(),
            mixed_tables,
            "invalid typed table configuration",
        ),
        (
            query,
            Value::from(private),
            Value::from(""),
            valid_key.clone(),
            invalid_names.clone(),
            "invalid journal identity",
        ),
        (
            query,
            Value::from("28A090CB-9741-43B0-8981-A9F030F6FC3C"),
            Value::from(SPACE),
            valid_key.clone(),
            invalid_names.clone(),
            "invalid journal identity or space",
        ),
        (
            query,
            Value::from(JOURNAL),
            Value::from(""),
            valid_key.clone(),
            invalid_names.clone(),
            "invalid journal identity or space",
        ),
        (
            query,
            Value::from(JOURNAL),
            Value::from(SPACE),
            valid_key.clone(),
            invalid_names,
            "invalid typed table configuration",
        ),
        (
            query,
            Value::from(JOURNAL),
            Value::from(SPACE),
            valid_key.clone(),
            Value::from(vec![Value::from("")]),
            "invalid typed table configuration",
        ),
    ];
    for (sql, journal, space, private_key, tables, expected_message) in cases {
        let error = db
            .query(sql)
            .bind(("journal", journal))
            .bind(("space", space))
            .bind(("key", private_key))
            .bind(("tables", tables))
            .bind((
                "value",
                reference("private_table", Id::String(private.into())),
            ))
            .await
            .unwrap()
            .check()
            .unwrap_err();
        let expected = surrealdb::Error::Db(surrealdb::error::Db::InvalidArguments {
            name: "recall::reference_tokens".into(),
            message: expected_message.into(),
        });
        assert_eq!(error.to_string(), expected.to_string());
        assert!(!error.to_string().contains(private));
        assert!(!error.to_string().contains("private_table"));
    }

    // Failed calls must not retain table configuration or poison a later call.
    let value = reference("recall_probe", Id::Number(42));
    let mut response = db
        .query(query)
        .bind(("journal", JOURNAL))
        .bind(("space", SPACE))
        .bind(("key", Value::Bytes(key().to_vec().into())))
        .bind(("tables", valid_tables))
        .bind(("value", value))
        .await
        .unwrap()
        .check()
        .unwrap();
    assert_eq!(response.take::<Vec<String>>(0).unwrap(), [NUMBER_TOKEN]);
}

#[test]
fn native_reference_rejects_uncomputed_and_excessively_nested_values() {
    let uncomputed = Value::Param(surrealdb::sql::Param::from("uncomputed"));
    assert!(recall_reference_tokens(JOURNAL, SPACE, &key(), &[], &uncomputed).is_err());
    let encoded = format!("{}\"recall_probe:42\"{}", "[".repeat(256), "]".repeat(256));
    assert!(recall_reference_tokens(JOURNAL, SPACE, &key(), &[], &Value::from(encoded)).is_err());
}

#[test]
#[expect(
    clippy::unwrap_used,
    reason = "Retain the original identity token fixture and nested-survivor membership assertions."
)]
fn native_reference_identity_membership_excludes_nested_survivors() {
    let inner = Thing::from(("recall_probe", Id::Number(42)));
    let outer = Thing::from(("extension", Id::Array(vec![Value::Thing(inner)].into())));
    let identity =
        surrealdb::recall_reference_identity_token(JOURNAL, SPACE, &key(), &[], &outer).unwrap();
    let postimage = tokens(&Value::Thing(outer));
    assert_eq!(postimage.len(), 2);
    assert!(postimage.contains(&identity));
    assert_ne!(identity, NUMBER_TOKEN);
}

#[tokio::test]
#[expect(
    clippy::unwrap_used,
    reason = "Retain the original native query and RPC request fixture construction used by the custody-redaction assertions."
)]
async fn native_reference_query_and_rpc_debug_redact_custody_bindings() {
    let db = Surreal::new::<Mem>(()).await.unwrap();
    let query = db
        .query("RETURN $ordinary;")
        .bind(("__recall_reference_key", "recall-key-debug-sentinel"))
        .bind(("__recall_journal_identity", "recall-journal-debug-sentinel"))
        .bind((
            "__recall_erasure_reservation",
            "recall-reservation-debug-sentinel",
        ))
        .bind(("ordinary", "ordinary-debug-sentinel"));
    let formatted = format!("{query:?}");
    assert!(!formatted.contains("recall-key-debug-sentinel"));
    assert!(!formatted.contains("recall-journal-debug-sentinel"));
    assert!(!formatted.contains("recall-reservation-debug-sentinel"));
    assert!(formatted.contains("ordinary-debug-sentinel"));
    assert!(formatted.contains("[REDACTED]"));

    let request = surrealdb::rpc::request::Request::try_from(object([
        ("method", Value::from("query")),
        (
            "params",
            Value::from(vec![
                Value::from("RETURN $ordinary;"),
                object([
                    (
                        "__recall_reference_key",
                        Value::from("recall-key-debug-sentinel"),
                    ),
                    (
                        "__recall_journal_identity",
                        Value::from("recall-journal-debug-sentinel"),
                    ),
                    ("ordinary", Value::from("ordinary-debug-sentinel")),
                ]),
            ]),
        ),
    ]))
    .unwrap();
    let formatted = format!("{request:?}");
    assert!(!formatted.contains("recall-key-debug-sentinel"));
    assert!(!formatted.contains("recall-journal-debug-sentinel"));
    assert!(!formatted.contains("recall-reservation-debug-sentinel"));
    assert!(formatted.contains("ordinary-debug-sentinel"));
    assert!(formatted.contains("[REDACTED]"));
}

#[tokio::test]
#[expect(
    clippy::unwrap_used,
    reason = "Retain original query-scoped reservation fixtures and response checks across the isolated function boundary."
)]
async fn native_reference_reservation_binding_crosses_isolated_scope_only_for_query() {
    let db = Surreal::new::<Mem>(()).await.unwrap();
    db.use_ns("recall")
        .use_db("reservation-context")
        .await
        .unwrap();
    let sql = "DEFINE FUNCTION fn::recall_owner() { RETURN $__recall_erasure_reservation; }; RETURN array::map([1], |$ignored| fn::recall_owner());";
    let mut response = db
        .query(sql)
        .bind(("__recall_erasure_reservation", "fixed-owner-sentinel"))
        .await
        .unwrap()
        .check()
        .unwrap();
    assert_eq!(
        response.take::<Vec<String>>(1).unwrap(),
        ["fixed-owner-sentinel"]
    );
    let mut response = db
        .query("RETURN fn::recall_owner();")
        .await
        .unwrap()
        .check()
        .unwrap();
    assert_eq!(response.take::<Option<String>>(0).unwrap(), None);
}

#[tokio::test]
#[expect(
    clippy::unwrap_used,
    reason = "Retain original response construction and statement-result assertions around borrowed error inspection."
)]
async fn native_reference_response_error_inspection_preserves_statement_results() {
    let db = Surreal::new::<Mem>(()).await.unwrap();
    db.use_ns("recall").use_db("borrow-errors").await.unwrap();
    let mut response = db
        .query("RETURN 7; THROW 'publication:erasure_space_reserved'; RETURN 9;")
        .await
        .unwrap();
    let observed = response
        .errors()
        .map(|(index, _)| index)
        .collect::<Vec<_>>();
    assert_eq!(observed, [1]);
    assert_eq!(response.errors().count(), 1);
    assert_eq!(response.take::<Option<i64>>(0).unwrap(), Some(7));
    assert_eq!(response.take::<Option<i64>>(2).unwrap(), Some(9));
    assert_eq!(response.take_errors().into_keys().collect::<Vec<_>>(), [1]);
}

// Retain the parser-first identity decision independently of the negative
// lexical filter. Encoded JSON is covered separately below.
fn original_parser_first_identity(value: &str) -> Option<Thing> {
    if let Ok(reference) = surrealdb::syn::thing(value) {
        if reference.to_string() == value {
            return Some(reference);
        }
    }
    let (table, key) = value.split_once(':')?;
    let table_valid = !table.is_empty()
        && table
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_');
    let key_valid = !key.is_empty()
        && key
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-');
    (table_valid && key_valid).then(|| Thing::from((table, key)))
}

#[tokio::test]
#[expect(
    clippy::map_unwrap_or,
    clippy::unwrap_used,
    reason = "Retain the independent parser-first oracle, exact map/unwrap-or expression and all direct-versus-SDK identity assertions."
)]
async fn native_reference_parser_filter_preserves_original_identity_precedence() {
    let db = Surreal::new::<Mem>(()).await.unwrap();
    db.use_ns("recall")
        .use_db("parser-filter-identities")
        .await
        .unwrap();
    let mut spellings = vec![
        format!("source:{JOURNAL}"),
        format!("extension:{JOURNAL}"),
        "source:FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF".into(),
        "source:12345678-1234-1234-1234-123456789abc".into(),
        "source:12345678-1234-1234-1234-123456789abz".into(),
        "source:12345678-1234-1234-1234-123456789ab".into(),
        "source:12345678-1234-1234-1234-123456789abcd".into(),
        "recall_probe:-123".into(),
        "recall_probe:-0".into(),
        "recall_probe:+123".into(),
        "recall_probe:00123".into(),
        "recall_probe:123".into(),
        "recall_probe:9223372036854775808".into(),
        "recall_probe:one-two".into(),
        "recall_probe:alpha".into(),
        "recall_probe:_".into(),
        "123:alpha".into(),
        "ordinary prose recall_probe:42 ending".into(),
        "recall_probe:42 /* ordinary prose */".into(),
        " recall_probe:42".into(),
        "recall_probe:42 ".into(),
        JOURNAL.into(),
    ];
    let native = [
        Thing::from(("source", Id::String(JOURNAL.into()))),
        Thing::from((
            "source",
            Id::Uuid(uuid::Uuid::parse_str(JOURNAL).unwrap().into()),
        )),
        Thing::from(("recall_probe", Id::String("123".into()))),
        Thing::from(("quoted table", Id::String("id`with:colon".into()))),
        Thing::from(("{quoted", Id::String(JOURNAL.into()))),
        Thing::from(("[quoted", Id::String(JOURNAL.into()))),
        Thing::from(("\"quoted", Id::String(JOURNAL.into()))),
        Thing::from((
            "extension",
            Id::Array(vec![Value::from("row"), 7_i64.into()].into()),
        )),
        Thing::from((
            "extension",
            Id::Object(Object::from(BTreeMap::<String, Value>::from([(
                "key".into(),
                Value::from(7_i64),
            )]))),
        )),
    ];
    spellings.extend(native.iter().map(ToString::to_string));
    assert!(matches!(
        original_parser_first_identity("recall_probe:-123")
            .unwrap()
            .id,
        Id::Number(-123)
    ));
    for tables in [vec![], vec!["source".to_owned()]] {
        for spelling in &spellings {
            let expected_value = original_parser_first_identity(spelling)
                .map(Value::Thing)
                .unwrap_or(Value::None);
            let expected =
                recall_reference_tokens(JOURNAL, SPACE, &key(), &tables, &expected_value).unwrap();
            let input = Value::from(spelling.clone());
            assert_eq!(
                recall_reference_tokens(JOURNAL, SPACE, &key(), &tables, &input).unwrap(),
                expected,
                "public parser-first identity changed for {spelling:?}"
            );
            let mut response = db
                .query("RETURN recall::reference_tokens($journal, $space, $key, $tables, $value);")
                .bind(("journal", JOURNAL))
                .bind(("space", SPACE))
                .bind(("key", Value::Bytes(key().to_vec().into())))
                .bind(("tables", tables.clone()))
                .bind(("value", input))
                .await
                .unwrap()
                .check()
                .unwrap();
            assert_eq!(response.take::<Vec<String>>(0).unwrap(), expected);
        }
    }
}

#[tokio::test]
#[expect(
    clippy::unwrap_used,
    reason = "Retain every original encoded fixture layer, direct-versus-SDK comparison and exact refusal boundary."
)]
async fn native_reference_parser_filter_preserves_encoded_forms_and_errors() {
    let db = Surreal::new::<Mem>(()).await.unwrap();
    db.use_ns("recall")
        .use_db("parser-filter-encoded")
        .await
        .unwrap();
    let tables = vec!["source".to_owned()];
    let source = Thing::from(("source", Id::String(JOURNAL.into())));
    let number = Thing::from(("recall_probe", Id::Number(42)));
    let expected = recall_reference_tokens(
        JOURNAL,
        SPACE,
        &key(),
        &tables,
        &Value::from(vec![Value::Thing(source), Value::Thing(number)]),
    )
    .unwrap();
    let encoded = serde_json::to_string(&serde_json::json!({
        "source": format!("source:{JOURNAL}"),
        "object": {"tb": "recall_probe", "id": 42},
        "nested": serde_json::to_string(&serde_json::json!([
            format!("source:{JOURNAL}"), "recall_probe:42"
        ])).unwrap()
    }))
    .unwrap();
    let variants = [
        encoded.clone(),
        format!(" \n\t{encoded}"),
        format!("[{encoded}]"),
        serde_json::to_string(&encoded).unwrap(),
        serde_json::to_string(&serde_json::to_string(&encoded).unwrap()).unwrap(),
    ];
    for spelling in variants {
        // The retained parser-first branch cannot recognize these complete
        // strings as canonical native record displays.
        assert!(original_parser_first_identity(&spelling).is_none());
        let input = Value::from(spelling);
        assert_eq!(
            recall_reference_tokens(JOURNAL, SPACE, &key(), &tables, &input).unwrap(),
            expected
        );
        let mut response = db
            .query("RETURN recall::reference_tokens($journal, $space, $key, $tables, $value);")
            .bind(("journal", JOURNAL))
            .bind(("space", SPACE))
            .bind(("key", Value::Bytes(key().to_vec().into())))
            .bind(("tables", tables.clone()))
            .bind(("value", input))
            .await
            .unwrap()
            .check()
            .unwrap();
        assert_eq!(response.take::<Vec<String>>(0).unwrap(), expected);
    }
    for malformed in ["{source:42}", "[source:42", "\"source:42", "{\"source\":"] {
        assert!(tokens(&Value::from(malformed)).is_empty());
    }
    let too_deep = format!("{}\"recall_probe:42\"{}", "[".repeat(256), "]".repeat(256));
    let error = recall_reference_tokens(
        JOURNAL,
        SPACE,
        &key(),
        &tables,
        &Value::from(too_deep.clone()),
    )
    .unwrap_err();
    let expected_error = surrealdb::error::Db::InvalidArguments {
        name: "recall::reference_tokens".into(),
        message: "encoded reference nesting limit exceeded".into(),
    };
    assert_eq!(error.to_string(), expected_error.to_string());
    let native_error = db
        .query("RETURN recall::reference_tokens($journal, $space, $key, $tables, $value);")
        .bind(("journal", JOURNAL))
        .bind(("space", SPACE))
        .bind(("key", Value::Bytes(key().to_vec().into())))
        .bind(("tables", tables))
        .bind(("value", too_deep))
        .await
        .unwrap()
        .check()
        .unwrap_err();
    assert_eq!(
        native_error.to_string(),
        surrealdb::Error::Db(expected_error).to_string()
    );
}
