//! Actual native queries retain missing-field tails and original error order.
use super::{array, exact_native, native, object, query_value};
use std::collections::BTreeMap;
use surrealdb::sql::Value;

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
async fn native_missing_optional_columns_and_method_tails_keep_none_semantics() {
    let client = native().await;
    let actual = query_value(
        &client,
        "LET $row = {}; RETURN [$row.missing, $row.missing.child, $row.id.child, $row.missing.is_none(), $row.missing.child.is_none()];",
        BTreeMap::new(),
    )
    .await
    .expect("native missing paths and methods");
    exact_native(
        &actual,
        &array([
            Value::None,
            Value::None,
            Value::None,
            true.into(),
            true.into(),
        ]),
    );
    client
        .query_checked("CREATE missing_columns:original SET present = 'retained';")
        .await
        .expect("actual native row omits optional columns");
    let projected = query_value(
        &client,
        "SELECT VALUE [present, absent, absent.child, absent.is_none()] FROM ONLY missing_columns:original;",
        BTreeMap::new(),
    )
    .await
    .expect("actual native document projection");
    exact_native(
        &projected,
        &array(["retained".into(), Value::None, Value::None, true.into()]),
    );
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
async fn native_missing_field_keeps_method_argument_and_unselected_sibling_errors() {
    let client = native().await;
    client
        .query_checked("DEFINE FUNCTION fn::missing_tail_error() { THROW 'original_missing_tail_argument'; }; DEFINE FUNCTION fn::missing_sibling_error() { THROW 'original_unselected_sibling'; };")
        .await
        .expect("actual original native error functions");
    let error = query_value(
        &client,
        "LET $row = {}; RETURN $row.missing.is_none(fn::missing_tail_error());",
        BTreeMap::new(),
    )
    .await
    .expect_err("missing field still evaluates the original method argument");
    assert!(
        error.to_string().contains("original_missing_tail_argument"),
        "method argument must precede argument-count validation: {error}"
    );
    let sibling =
        surrealdb::sql::value("fn::missing_sibling_error()").expect("deferred sibling native AST");
    let error = query_value(
        &client,
        "RETURN $row.missing;",
        BTreeMap::from([("row".into(), object([("unselected", sibling)]))]),
    )
    .await
    .expect_err("complete local evaluation still computes the unselected sibling");
    assert!(
        error.to_string().contains("original_unselected_sibling"),
        "missing lookup must not bypass original local evaluation: {error}"
    );
}
