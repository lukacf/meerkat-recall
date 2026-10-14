//! Native document dispatch controls with real retries, events and rollback.
use super::{array, exact_native, native, query_value};
use std::collections::BTreeMap;
use surrealdb::sql::{Thing, Value};

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
async fn native_document_dispatch_keeps_crud_retry_events_and_relation_results() {
    let client = native().await;
    client
        .query_checked(
            r"CREATE document_dispatch_trace:order SET events=[];
        DEFINE EVENT document_dispatch_event ON TABLE document_dispatch_row WHEN true THEN (
            UPDATE document_dispatch_trace:order
                SET events=array::append(events, $event) RETURN NONE
        );",
        )
        .await
        .expect("native ordered event fixture");
    let result = query_value(
        &client,
        r"BEGIN TRANSACTION;
        CREATE document_dispatch_row:a SET n=0;
        UPDATE document_dispatch_row:a SET n += 1;
        UPSERT document_dispatch_row:a SET n += 1;
        INSERT INTO document_dispatch_row (id, n) VALUES ('a', 99)
            ON DUPLICATE KEY UPDATE n += 1;
        INSERT INTO document_dispatch_row (id, n) VALUES ('b', 7);
        LET $edge = RELATE document_dispatch_row:a->document_dispatch_link:edge->document_dispatch_row:b
            SET label='linked' RETURN VALUE label;
        DELETE document_dispatch_link:edge RETURN NONE;
        DELETE document_dispatch_row:b RETURN NONE;
        RETURN [
            (SELECT VALUE n FROM ONLY document_dispatch_row:a),
            (SELECT VALUE events FROM ONLY document_dispatch_trace:order),
            $edge,
            (SELECT * FROM ONLY document_dispatch_row:b),
            (SELECT * FROM ONLY document_dispatch_link:edge)
        ];
        COMMIT TRANSACTION;",
        BTreeMap::new(),
    )
    .await
    .expect("all seven document operations keep their real native semantics");
    exact_native(
        &result,
        &array([
            3_i64.into(),
            array([
                "CREATE".into(),
                "UPDATE".into(),
                "UPDATE".into(),
                "UPDATE".into(),
                "CREATE".into(),
                "DELETE".into(),
            ]),
            array(["linked".into()]),
            Value::None,
            Value::None,
        ]),
    );
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
async fn native_document_dispatch_keeps_unique_retry_identity_and_transaction_rollback() {
    let client = native().await;
    client
        .query_checked(
            r"CREATE document_dispatch_retry_trace:events SET count=0;
        DEFINE INDEX document_dispatch_unique ON TABLE document_dispatch_retry FIELDS external UNIQUE;
        DEFINE EVENT document_dispatch_retry_event ON TABLE document_dispatch_retry WHEN true THEN (
            UPDATE document_dispatch_retry_trace:events SET count += 1 RETURN NONE
        );
        CREATE document_dispatch_retry:original SET external='stable', n=1;
        INSERT INTO document_dispatch_retry (external, n) VALUES ('stable', 99)
            ON DUPLICATE KEY UPDATE n += 1;",
        )
        .await
        .expect("unique-key retry updates the original record once");
    let state_query = r"RETURN [
        (SELECT VALUE id FROM document_dispatch_retry),
        (SELECT VALUE n FROM ONLY document_dispatch_retry:original),
        (SELECT VALUE count FROM ONLY document_dispatch_retry_trace:events)
    ];";
    let expected = array([
        array([Value::Thing(Thing::from((
            "document_dispatch_retry",
            "original",
        )))]),
        2_i64.into(),
        2_i64.into(),
    ]);
    let before = query_value(&client, state_query, BTreeMap::new())
        .await
        .expect("native state after successful retry");
    exact_native(&before, &expected);

    let error = client
        .query_checked(
            r"BEGIN TRANSACTION;
        UPDATE document_dispatch_retry:original SET n += 1 RETURN NONE;
        INSERT INTO document_dispatch_retry (external, n) VALUES ('stable', 99)
            ON DUPLICATE KEY UPDATE n += 1;
        THROW 'document_dispatch_original_error';
        COMMIT TRANSACTION;",
        )
        .await
        .expect_err("the original first error aborts mutations and retry events")
        .to_string();
    assert!(
        error.contains("document_dispatch_original_error"),
        "{error}"
    );
    let after = query_value(&client, state_query, BTreeMap::new())
        .await
        .expect("native state after failed transaction");
    exact_native(&after, &expected);
}
