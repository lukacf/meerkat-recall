//! Original writer nonce fields and replay completion retain exact native values.

use super::*;

fn epoch(fixture: &Fixture, fields: &[&str]) -> RecallHistoryRecord {
    let mut value = object([
        ("space_id", SPACE.into()),
        ("epoch", uuid::Uuid::new_v4().to_string().into()),
    ]);
    let Value::Object(values) = &mut value else {
        unreachable!();
    };
    for field in fields {
        values.insert((*field).into(), uuid::Uuid::new_v4().to_string().into());
    }
    let row = native(control_thing("canonical_epoch", SPACE), value, 2, true);
    assert!(fixture.authority.version_commitment(&row).is_ok());
    row
}

#[test]
fn original_head_and_epoch_reader_nonces_retain_complete_native_commitments() {
    let mut fixture = Fixture::new();
    let before = fixture
        .authority
        .version_commitment(&fixture.rows[0])
        .unwrap();
    edit(&mut fixture.rows[0], |fields| {
        fields.insert(
            "fence_nonce".into(),
            uuid::Uuid::new_v4().to_string().into(),
        );
        fields.insert(
            "canonical_read_nonce".into(),
            uuid::Uuid::new_v4().to_string().into(),
        );
    });
    let after = fixture
        .authority
        .version_commitment(&fixture.rows[0])
        .unwrap();
    assert_ne!(before, after);
    let retained = fixture.result().unwrap();
    assert!(retained.contains(&after));
    assert!(
        !retained.contains(&before),
        "nonce bytes must not be stripped"
    );

    for names in [
        vec!["standing_watch_read_nonce"],
        vec!["queue_read_nonce"],
        vec!["standing_watch_read_nonce", "queue_read_nonce"],
    ] {
        let mut fixture = Fixture::new();
        let row = epoch(&fixture, &names);
        let proof = fixture.authority.version_commitment(&row).unwrap();
        fixture.rows.push(row);
        assert!(fixture.result().unwrap().contains(&proof));
    }
}

#[test]
fn original_reader_nonce_fields_refuse_wrong_table_type_and_payload() {
    let invalid_values = [
        Value::from("source-payload-canary"),
        Value::from("AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA"),
        Value::from("00000000000040008000000000000000"),
        Value::from(17_i64),
        Value::Uuid(surrealdb::sql::Uuid::from(uuid::Uuid::new_v4())),
    ];
    for field in [
        "fence_nonce",
        "standing_watch_read_nonce",
        "queue_read_nonce",
    ] {
        for value in &invalid_values {
            let mut fixture = Fixture::new();
            let index = if field == "fence_nonce" {
                0
            } else {
                let row = epoch(&fixture, &[]);
                fixture.rows.push(row);
                fixture.rows.len() - 1
            };
            edit(&mut fixture.rows[index], |fields| {
                fields.insert(field.into(), value.clone());
            });
            assert!(fixture.result().is_err(), "{field}: {value:?}");
        }
    }
    for (index, field) in [
        (1, "fence_nonce"),
        (2, "fence_nonce"),
        (0, "standing_watch_read_nonce"),
        (0, "queue_read_nonce"),
    ] {
        let mut fixture = Fixture::new();
        edit(&mut fixture.rows[index], |fields| {
            fields.insert(field.into(), uuid::Uuid::new_v4().to_string().into());
        });
        assert!(fixture.result().is_err(), "row {index}: {field}");
    }
    let mut fixture = Fixture::new();
    let mut row = epoch(&fixture, &["standing_watch_read_nonce", "queue_read_nonce"]);
    edit(&mut row, |fields| {
        fields.insert("hidden_source_text".into(), "must-not-survive".into());
    });
    fixture.rows.push(row);
    assert!(fixture.result().is_err());
}

fn restored_completion() -> Fixture {
    let mut fixture = with_completion();
    edit(&mut fixture.rows[2], |fields| {
        let mut operation: StoredOperation =
            serde_json::from_str(text(fields, "payload").unwrap()).unwrap();
        // Original replay sanitizes the accepted operation and rotates the head;
        // it preserves this original receipt identity and source generation.
        operation.receipt.state = OperationState::Superseded;
        operation.receipt.source_generation = uuid::Uuid::new_v4().to_string();
        fields.insert("payload".into(), encode_payload(&operation).unwrap().into());
        fields.insert(
            "state".into(),
            crate::surreal::stored_native_value(
                crate::surreal::json_to_surreal(serde_json::to_value(operation).unwrap()).unwrap(),
            ),
        );
    });
    fixture
}

#[test]
fn restored_superseded_original_operation_authorizes_exact_current_completion() {
    let mut fixture = restored_completion();
    let current = fixture.rows.last().unwrap().clone();
    let proof = fixture.authority.version_commitment(&current).unwrap();
    assert_eq!(fixture.result().unwrap().len(), 7);
    assert!(fixture.result().unwrap().contains(&proof));
    let mut historical = current;
    historical.current = false;
    historical.version = 1;
    historical.timestamp = 1;
    let old_proof = fixture.authority.version_commitment(&historical).unwrap();
    fixture.rows.push(historical);
    assert!(!fixture.result().unwrap().contains(&old_proof));
}

#[test]
fn restored_completion_still_requires_sanitized_receipt_and_current_erased_head() {
    for mode in 0..6 {
        let mut fixture = restored_completion();
        if mode < 4 {
            edit(&mut fixture.rows[2], |fields| {
                let mut operation: StoredOperation =
                    serde_json::from_str(text(fields, "payload").unwrap()).unwrap();
                match mode {
                    0 => operation.receipt.completed_at = None,
                    1 => operation.receipt.desired_revision = Some("old-source-payload".into()),
                    2 => operation.receipt.error_code = Some("old-provider-payload".into()),
                    _ => operation.receipt.state = OperationState::Completed,
                }
                fields.insert("payload".into(), encode_payload(&operation).unwrap().into());
                fields.insert(
                    "state".into(),
                    crate::surreal::stored_native_value(
                        crate::surreal::json_to_surreal(serde_json::to_value(operation).unwrap())
                            .unwrap(),
                    ),
                );
            });
        } else {
            edit(&mut fixture.rows[0], |fields| {
                let mut head: SourceHead =
                    serde_json::from_str(text(fields, "payload").unwrap()).unwrap();
                if mode == 4 {
                    head.lifecycle = SourceVisibility::Erasing;
                } else {
                    head.operation_id = uuid::Uuid::new_v4().to_string();
                }
                fields.insert("payload".into(), encode_payload(&head).unwrap().into());
                fields.insert(
                    "state".into(),
                    crate::surreal::stored_native_value(
                        crate::surreal::json_to_surreal(serde_json::to_value(head).unwrap())
                            .unwrap(),
                    ),
                );
            });
        }
        assert!(fixture.result().is_err(), "replay mutation {mode}");
    }
}
