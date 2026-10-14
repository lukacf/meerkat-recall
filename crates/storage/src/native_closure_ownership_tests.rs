//! Native behavior controls for borrowed closure bodies and owned arguments.
//! Fixed results and durable effects are the oracle, not allocation counts.

use super::{array, exact_native, native, object, query_value};
use crate::StorageClient;
use std::collections::BTreeMap;
use surrealdb::sql::Value;

async fn install_trace(client: &StorageClient) {
    client.query_checked(
        "DEFINE TABLE closure_ownership_trace SCHEMALESS; DEFINE FUNCTION fn::closure_ownership_effect($label: string, $value: any) { LET $ordinal = array::len((SELECT id FROM closure_ownership_trace)); CREATE closure_ownership_trace SET label=$label, ordinal=$ordinal; RETURN $value; };",
    ).await.expect("native durable closure trace");
}

async fn assert_trace(client: &StorageClient, labels: &[&str]) {
    let actual = query_value(
        client,
        "SELECT label, ordinal FROM closure_ownership_trace ORDER BY ordinal;",
        BTreeMap::new(),
    )
    .await
    .expect("actual durable trace order");
    let expected = array(labels.iter().enumerate().map(|(ordinal, label)| {
        object([
            ("label", Value::from(*label)),
            (
                "ordinal",
                Value::from(i64::try_from(ordinal).expect("small trace")),
            ),
        ])
    }));
    exact_native(&actual, &expected);
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
async fn native_fold_borrowed_body_preserves_accumulator_indices_and_effect_order() {
    let client = native().await;
    install_trace(&client).await;
    let result = query_value(
        &client,
        r"BEGIN TRANSACTION;
        RETURN array::fold([2, 4, 6], {sum:0, indices:[], values:[]},
            |$acc: object, $value: int, $index: int| {
                LET $observed = fn::closure_ownership_effect(<string>$index, $value);
                RETURN {sum:$acc.sum + $observed,
                    indices:array::append($acc.indices, $index),
                    values:array::append($acc.values, $observed)};
            });
        COMMIT TRANSACTION;",
        BTreeMap::new(),
    )
    .await
    .expect("native fold with owned accumulator and ordered effects");
    exact_native(
        &result,
        &object([
            ("sum", 12_i64.into()),
            ("indices", array([0_i64.into(), 1_i64.into(), 2_i64.into()])),
            ("values", array([2_i64.into(), 4_i64.into(), 6_i64.into()])),
        ]),
    );
    assert_trace(&client, &["0", "1", "2"]).await;
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
async fn native_filter_borrowed_body_preserves_member_evaluation_and_surviving_value() {
    let client = native().await;
    install_trace(&client).await;
    let result = query_value(
        &client,
        r"BEGIN TRANSACTION;
        RETURN array::filter([
            {index:0, value:fn::closure_ownership_effect('first', 7)},
            {index:1, value:fn::closure_ownership_effect('second', 9)}
        ], |$row: object| fn::closure_ownership_effect('predicate', $row.index = 1));
        COMMIT TRANSACTION;",
        BTreeMap::new(),
    )
    .await
    .expect("native filter with ordered member and predicate effects");
    exact_native(
        &result,
        &array([object([("index", 1_i64.into()), ("value", 9_i64.into())])]),
    );
    assert_trace(&client, &["first", "second", "predicate", "predicate"]).await;
    let equal = query_value(
        &client,
        "RETURN [array::filter([1, 2, 1], 1), array::filter([NONE, NULL, NONE], NONE), array::filter([], |$value| { THROW 'empty_filter_body'; })];",
        BTreeMap::new(),
    )
    .await
    .expect("non-closure equality and empty closure filter retain their branches");
    exact_native(
        &equal,
        &array([
            array([1_i64.into(), 1_i64.into()]),
            array([Value::None, Value::None]),
            array([]),
        ]),
    );
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
async fn native_closure_owned_arguments_keep_optional_missing_and_ignored_extra_values() {
    let client = native().await;
    let result = query_value(
        &client,
        r"LET $call = |$required: int, $optional: option<int>| [$required, $optional];
        RETURN [
            $call(7),
            $call(7, 8, 'ignored'),
            array::fold([2, 4], 0, |$acc: int, $value: int| $acc + $value),
            array::fold([2, 4], 0, |$acc: int, $value: int, $index: int, $missing: option<int>| {
                IF $missing != NONE { THROW 'optional_not_missing'; };
                RETURN $acc + $value + $index;
            }),
            array::fold([], 'unchanged', |$acc, $value| { THROW 'empty_fold_body'; })
        ];",
        BTreeMap::new(),
    )
    .await
    .expect("native optional, extra and empty-fold contracts");
    exact_native(
        &result,
        &array([
            array([7_i64.into(), Value::None]),
            array([7_i64.into(), 8_i64.into()]),
            6_i64.into(),
            7_i64.into(),
            "unchanged".into(),
        ]),
    );

    let error = query_value(
        &client,
        "LET $call = |$required: int| { THROW 'missing_argument_body'; }; RETURN $call();",
        BTreeMap::new(),
    )
    .await
    .expect_err("missing required argument refuses before the body")
    .to_string();
    assert!(error.contains("Expected a value for $required"), "{error}");
    assert!(!error.contains("missing_argument_body"), "{error}");
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
async fn native_closure_owned_arguments_keep_coercion_return_checks_and_first_error() {
    let client = native().await;
    for sql in [
        "RETURN array::fold(['wrong'], 0, |$acc: int, $value: int| { THROW 'coercion_body'; });",
        "RETURN array::filter(['wrong'], |$value: int| { THROW 'coercion_body'; });",
    ] {
        let error = query_value(&client, sql, BTreeMap::new())
            .await
            .expect_err("native argument coercion must precede body execution")
            .to_string();
        assert!(
            error.contains("Expected a value of type 'int' for argument $`value`"),
            "{error}"
        );
        assert!(!error.contains("coercion_body"), "{error}");
    }
    for sql in [
        "RETURN array::fold([1], 0, |$acc: int, $value: int| -> int { RETURN 'wrong'; });",
        "RETURN array::filter([1], |$value: int| -> int { RETURN 'wrong'; });",
    ] {
        let error = query_value(&client, sql, BTreeMap::new())
            .await
            .expect_err("native declared return type cannot be skipped")
            .to_string();
        assert!(
            error.contains("ANONYMOUS") && error.contains("int"),
            "{error}"
        );
    }
    let result = query_value(
        &client,
        "RETURN array::fold([1, 2], <decimal>0, |$acc: decimal, $value: decimal| -> decimal { RETURN $acc + $value; });",
        BTreeMap::new(),
    )
    .await
    .expect("native numeric coercion still returns the declared native representation");
    let decimal = query_value(&client, "RETURN <decimal>3;", BTreeMap::new())
        .await
        .expect("independent native decimal result");
    exact_native(&result, &decimal);
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
async fn native_borrowed_closure_keeps_future_effects_and_transaction_rollback() {
    let client = native().await;
    install_trace(&client).await;
    let fold = "array::fold([1, 2, 3], 0, |$acc: int, $value: int| <future> { fn::closure_ownership_effect('future', $acc + $value) })";
    let result = query_value(
        &client,
        &format!("BEGIN TRANSACTION; RETURN {fold}; COMMIT TRANSACTION;"),
        BTreeMap::new(),
    )
    .await
    .expect("anonymous-function futures remain enabled");
    exact_native(&result, &6_i64.into());
    assert_trace(&client, &["future", "future", "future"]).await;
    client
        .query_checked("DELETE closure_ownership_trace;")
        .await
        .expect("reset durable trace");
    let error = client.query_checked(&format!(
        "BEGIN TRANSACTION; LET $computed = {fold}; IF $computed != 6 {{ THROW 'wrong_fold_before_rollback'; }}; THROW 'closure_ownership_original_rollback'; COMMIT TRANSACTION;"
    )).await.expect_err("original later transaction throw").to_string();
    assert!(
        error.contains("closure_ownership_original_rollback"),
        "{error}"
    );
    assert_trace(&client, &[]).await;
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
async fn native_closure_argument_names_bind_raw_reserved_and_quoted_identifiers() {
    let client = native().await;
    let result = query_value(
        &client,
        r"LET $call = |$value: int, $`select`: int, $`two words`: int| [$value, $`select`, $`two words`];
        RETURN [
            $call(2, 3, 4),
            array::fold([5], 0, |$value: int, $`two words`: int| $value + $`two words`),
            array::filter([7, 9], |$value: int| $value = 9)
        ];",
        BTreeMap::new(),
    )
    .await
    .expect("raw parameter identity is distinct from quoted SQL display");
    exact_native(
        &result,
        &array([
            array([2_i64.into(), 3_i64.into(), 4_i64.into()]),
            5_i64.into(),
            array([9_i64.into()]),
        ]),
    );
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
async fn native_borrowed_closure_uses_current_body_and_keeps_isolated_scope() {
    let client = native().await;
    for (body, expected) in [
        ("|$acc, $value| $acc + $value", 6_i64),
        ("|$acc, $value| $acc + 2 * $value", 12_i64),
        ("|$acc, $value| $acc + $value", 6_i64),
    ] {
        let result = query_value(
            &client,
            "RETURN array::fold([1, 2, 3], 0, $current_mapper);",
            BTreeMap::from([(
                "current_mapper".into(),
                surrealdb::sql::value(body).expect("native closure AST"),
            )]),
        )
        .await
        .expect("each query uses its current bound closure body");
        exact_native(&result, &expected.into());
    }
    let result = query_value(
        &client,
        "LET $outside = 99; RETURN array::fold([7], NONE, |$acc, $value| [$value, $outside]);",
        BTreeMap::new(),
    )
    .await
    .expect("the original closure context remains isolated");
    exact_native(&result, &array([7_i64.into(), Value::None]));
}
