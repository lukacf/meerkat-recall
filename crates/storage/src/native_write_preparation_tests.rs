//! Native write preparation and the exact production byte-key formatter.
use super::{array, exact_native, native, object, query_value};
use std::collections::BTreeMap;
use surrealdb::sql::Value;

// Compile the actual dependency source, not a rewritten approximation. This
// module has no crate-private dependencies and introduces no runtime test mode.
#[rustfmt::skip]
#[path = "../../../vendor/surrealdb-core-2.6.5/src/key/debug.rs"]
mod native_key_debug;

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
async fn native_empty_field_set_preserves_none_cleanup_and_event_rollback() {
    let client = native().await;
    client
        .query_checked(
            r"DEFINE TABLE write_preparation_empty SCHEMALESS;
            CREATE write_preparation_trace:original SET events=[];
            DEFINE EVENT write_preparation_event ON TABLE write_preparation_empty
                WHEN true THEN (
                    UPDATE write_preparation_trace:original
                        SET events=array::append(events, $event) RETURN NONE
                );
            CREATE write_preparation_empty:original
                SET nonce='first', removed='old';
            UPDATE write_preparation_empty:original
                SET nonce='second', removed=NONE,
                    payload={keep:17, discarded:NONE},
                    bag=[NONE, NULL, {keep:19, discarded:NONE}], empty={};",
        )
        .await
        .expect("native schemaless writes and their original events");
    let state = r"RETURN [
        (SELECT VALUE [nonce, payload, bag, removed, empty]
            FROM ONLY write_preparation_empty:original),
        (SELECT VALUE events FROM ONLY write_preparation_trace:original)
    ];";
    let expected = array([
        array([
            "second".into(),
            object([("keep", 17_i64.into())]),
            array([Value::None, Value::Null, object([("keep", 19_i64.into())])]),
            Value::None,
            object(Vec::<(String, Value)>::new()),
        ]),
        array(["CREATE".into(), "UPDATE".into()]),
    ]);
    let before = query_value(&client, state, BTreeMap::new())
        .await
        .expect("native NONE cleanup preserves array positions and object fields");
    exact_native(&before, &expected);

    let failure = client
        .query_checked(
            r"BEGIN TRANSACTION;
            UPDATE write_preparation_empty:original SET nonce='rolled-back';
            THROW 'write_preparation_original_failure';
            COMMIT TRANSACTION;",
        )
        .await
        .expect_err("native mutation and its event must roll back together");
    assert!(
        failure
            .to_string()
            .contains("write_preparation_original_failure"),
        "{failure}"
    );
    let after = query_value(&client, state, BTreeMap::new())
        .await
        .expect("state remains readable after the original transaction error");
    exact_native(&after, &expected);
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
async fn native_empty_field_set_reloads_later_definitions_and_original_input() {
    let client = native().await;
    client
        .query_checked(
            r"DEFINE TABLE write_preparation_late SCHEMALESS;
            CREATE write_preparation_late:original SET note='first';
            UPDATE write_preparation_late:original SET note='before-definition';
            DEFINE FIELD note ON write_preparation_late TYPE string
                VALUE IF $input = NONE THEN $before ELSE string::lowercase($input) END;
            DEFINE FIELD immutable ON write_preparation_late TYPE string
                READONLY DEFAULT 'retained';
            UPDATE write_preparation_late:original SET note='AFTER-DEFINITION';
            UPDATE write_preparation_late:original SET nonce='unrelated';
            CREATE write_preparation_late:with_defaults SET note='NEW';",
        )
        .await
        .expect("an earlier empty field lookup cannot hide later definitions");
    let actual = query_value(
        &client,
        r"RETURN [
            (SELECT VALUE note FROM ONLY write_preparation_late:original),
            (SELECT VALUE [note, immutable] FROM ONLY write_preparation_late:with_defaults)
        ];",
        BTreeMap::new(),
    )
    .await
    .expect("VALUE retains changed input and unchanged original field context");
    exact_native(
        &actual,
        &array([
            "after-definition".into(),
            array(["new".into(), "retained".into()]),
        ]),
    );
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
async fn native_defined_field_assertions_and_readonly_errors_still_rollback() {
    let client = native().await;
    client
        .query_checked(
            r"DEFINE TABLE write_preparation_fields SCHEMALESS;
            DEFINE FIELD positive ON write_preparation_fields TYPE int ASSERT $value > 0;
            DEFINE FIELD immutable ON write_preparation_fields TYPE string
                READONLY DEFAULT 'original';
            CREATE write_preparation_fields:original SET positive=7;",
        )
        .await
        .expect("native original field assertions and readonly default");
    for (sql, expected_field) in [
        (
            "UPDATE write_preparation_fields:original SET positive=-1;",
            "positive",
        ),
        (
            "UPDATE write_preparation_fields:original SET immutable='changed';",
            "immutable",
        ),
    ] {
        let error = client.query_checked(sql).await.expect_err(expected_field);
        assert!(error.to_string().contains(expected_field), "{error}");
    }
    let actual = query_value(
        &client,
        "SELECT VALUE [positive, immutable] FROM ONLY write_preparation_fields:original;",
        BTreeMap::new(),
    )
    .await
    .expect("both field failures preserve the original native row");
    exact_native(&actual, &array([7_i64.into(), "original".into()]));
}

#[test]
fn native_key_sprint_matches_original_escape_for_every_byte_and_range() {
    use native_key_debug::Sprintable;

    fn original(bytes: &[u8]) -> String {
        bytes
            .iter()
            .flat_map(|&byte| std::ascii::escape_default(byte))
            .map(|byte| byte as char)
            .collect()
    }

    let every_byte: Vec<u8> = (0..=255).collect();
    for byte in &every_byte {
        let input = [*byte];
        assert_eq!(input.as_slice().sprint(), original(&input));
        assert_eq!(input.to_vec().sprint(), original(&input));
    }
    let inputs = [
        Vec::new(),
        every_byte.clone(),
        every_byte.into_iter().rev().collect(),
        b"/namespace/database/table/printable-record-key\x00\t\n\r'\"\\\xff".to_vec(),
        vec![b'a'; 4097],
    ];
    for input in inputs {
        assert_eq!(input.sprint(), original(&input));
        assert_eq!(input.as_slice().sprint(), original(&input));
        let end = [input.as_slice(), &[0xff]].concat();
        assert_eq!(
            (input.clone()..end.clone()).sprint(),
            format!("{}..{}", original(&input), original(&end))
        );
        assert_eq!(
            vec![input.clone(), end.clone()].sprint(),
            format!("{} + {}", original(&input), original(&end))
        );
    }
    let text = "ordinary unchanged unicode Å and newline\n";
    assert_eq!(text.sprint(), text);
    assert_eq!(text.to_owned().sprint(), text);
}
