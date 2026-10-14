//! Durable native evaluation boundaries used by producer ancestry guards.
use super::{array, exact_native, native, object, query_value};
use std::collections::BTreeMap;
use surrealdb::sql::Value;

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
async fn native_dispatch_keeps_select_then_fresh_update_same_row_future_effects() {
    let client = native().await;
    client
        .query_checked(
            r"CREATE dispatch_original:row SET mutations=0;
        UPDATE dispatch_original:row SET observed=<future> {
            UPDATE dispatch_original:row SET mutations += 1 RETURN NONE;
            RETURN 'original';
        } RETURN NONE;",
        )
        .await
        .expect("stored future with a real same-row write");
    let result = query_value(
        &client,
        r"BEGIN TRANSACTION;
        LET $before = SELECT * FROM ONLY dispatch_original:row;
        IF $before.observed != 'original' { THROW 'wrong_original'; };
        UPDATE dispatch_original:row SET nonce='touched';
        COMMIT TRANSACTION;
        SELECT mutations, nonce FROM ONLY dispatch_original:row;",
        BTreeMap::new(),
    )
    .await
    .expect("original separate read then fresh nonce update");
    exact_native(
        &result,
        &object([("mutations", 2_i64.into()), ("nonce", "touched".into())]),
    );
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
async fn native_dispatch_keeps_unconditional_cold_read_and_update_output_order() {
    let client = native().await;
    client
        .query_checked(
            r"CREATE dispatch_trace:order SET labels=[];
        DEFINE FUNCTION fn::dispatch_effect($label: string) {
            UPDATE dispatch_trace:order SET labels=array::append(labels, $label) RETURN NONE;
            RETURN $label;
        };
        CREATE dispatch_hot:row SET space_id='dispatch', payload='original';
        CREATE dispatch_cold:row SET space_id='dispatch';
        UPDATE dispatch_hot:row SET extra=<future> { fn::dispatch_effect('hot') } RETURN NONE;
        UPDATE dispatch_cold:row SET extra=<future> { fn::dispatch_effect('cold') } RETURN NONE;
        DEFINE EVENT dispatch_nonce ON TABLE dispatch_hot WHEN $event='UPDATE'
            THEN fn::dispatch_effect('event');",
        )
        .await
        .expect("actual hot/cold futures and nonce event");
    let result = query_value(
        &client,
        r"BEGIN TRANSACTION;
        LET $hot = SELECT * FROM ONLY dispatch_hot:row;
        LET $cold = SELECT * FROM ONLY dispatch_cold:row;
        LET $current = IF $hot != NONE THEN $hot ELSE $cold.record END;
        IF !recall::original_row_matches($current, 'dispatch', {payload:'original'}) {
            THROW 'wrong_original';
        };
        IF $hot != NONE { UPDATE dispatch_hot:row SET nonce='touched'; }
            ELSE { UPDATE dispatch_cold:row SET nonce='touched'; };
        COMMIT TRANSACTION;
        SELECT VALUE labels FROM ONLY dispatch_trace:order;",
        BTreeMap::new(),
    )
    .await
    .expect("original native guard boundaries");
    exact_native(
        &result,
        &array(["hot".into(), "cold".into(), "event".into(), "hot".into()]),
    );
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
async fn native_dispatch_keeps_future_error_and_all_prior_writes_in_rollback() {
    let client = native().await;
    client
        .query_checked(
            r"CREATE dispatch_rollback:row SET writes=0;
        CREATE dispatch_failure:row SET marker='before';
        UPDATE dispatch_failure:row SET effect=<future> {
            UPDATE dispatch_rollback:row SET writes += 1 RETURN NONE;
            THROW 'dispatch_original_future_error';
        } RETURN NONE;",
        )
        .await
        .expect("stored failing future without early evaluation");
    let error = client
        .query_checked(
            r"BEGIN TRANSACTION;
        UPDATE dispatch_rollback:row SET writes += 1 RETURN NONE;
        LET $current = SELECT * FROM ONLY dispatch_failure:row;
        UPDATE dispatch_failure:row SET marker='after' RETURN NONE;
        COMMIT TRANSACTION;",
        )
        .await
        .expect_err("the first native future error must abort")
        .to_string();
    assert!(error.contains("dispatch_original_future_error"), "{error}");
    let result = query_value(
        &client,
        "RETURN [(SELECT VALUE writes FROM ONLY dispatch_rollback:row), (SELECT VALUE marker FROM ONLY dispatch_failure:row)];",
        BTreeMap::new(),
    ).await.expect("actual durable state after rollback");
    exact_native(&result, &array([Value::from(0_i64), Value::from("before")]));
}
