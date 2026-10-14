//! Native codec fixtures and independent malformed-postimage controls.
#![allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]

use super::*;
use elephant_types::publication::{OperationReceipt, SourceProducerOrigin};
use elephant_types::records::{Security, SubjectsState};

const NS: &str = "history-control-ns";
const DB: &str = "history-control-db";
const SPACE: &str = "named-history-space";

fn object(fields: impl IntoIterator<Item = (&'static str, Value)>) -> Value {
    Value::Object(
        fields
            .into_iter()
            .map(|(key, value)| (key.into(), value))
            .collect(),
    )
}

#[expect(
    clippy::needless_pass_by_value,
    reason = "Keep the fixture-owned native value and authority construction interface unchanged."
)]
fn native(identity: Thing, value: Value, version: u64, current: bool) -> RecallHistoryRecord {
    let mut value = crate::surreal::stored_native_value(value);
    let Value::Object(fields) = &mut value else {
        panic!("native fixture object required");
    };
    fields.insert("id".into(), Value::Thing(identity.clone()));
    RecallHistoryRecord {
        key: surrealdb::kvs::recall_history::record_key(NS, DB, &identity.tb, &identity.id)
            .unwrap(),
        value: RecallHistoryRecord::encode_value(&value).unwrap(),
        version,
        timestamp: version,
        tombstone: false,
        current,
    }
}

fn edit(row: &mut RecallHistoryRecord, mutate: impl FnOnce(&mut Object)) {
    let Value::Object(mut fields) = row.decoded_value().unwrap() else {
        panic!("fixture object");
    };
    mutate(&mut fields);
    row.value = RecallHistoryRecord::encode_value(&Value::Object(fields)).unwrap();
}

struct Fixture {
    directory: tempfile::TempDir,
    journal: ErasureJournal,
    intent: ErasureIntent,
    authority: RecallHistoryAuthority,
    rows: Vec<RecallHistoryRecord>,
}

impl Fixture {
    #[expect(
        clippy::too_many_lines,
        reason = "Keep the complete ordered conformance scenario and its failure assertions together."
    )]
    fn new() -> Self {
        let directory = tempfile::tempdir().unwrap();
        let journal = ErasureJournal::open(directory.path()).unwrap();
        let source = RecordId::new_v4(TableName::Source);
        let intent = journal.record_intent(SPACE, &source).unwrap();
        let token = crate::reference_fence::reference_identity_token(
            journal.identity(),
            SPACE,
            journal.reference_key_bytes(),
            &crate::surreal::thing_from_record_id(&source),
        )
        .unwrap();
        journal
            .record_reference_membership(&intent, &BTreeSet::from([token.clone()]))
            .unwrap();
        let authority = RecallHistoryAuthority::new(
            NS.into(),
            DB.into(),
            journal.identity().into(),
            SPACE.into(),
            *journal.reference_key_bytes(),
            TYPED_UUID_TABLES
                .iter()
                .map(|table| (*table).into())
                .collect(),
            BTreeSet::from([token.clone()]),
        )
        .unwrap();
        let head = SourceHead {
            space_id: SPACE.into(),
            source_id: source.clone(),
            generation: uuid::Uuid::new_v4().to_string(),
            desired_revision: None,
            published_revision: None,
            operation_id: stable_key(&[SPACE, "erase-operation"]),
            lifecycle: SourceVisibility::Erasing,
            updated_at: intent.recorded_at,
        };
        let authorization = SourceAuthorizationEnvelope {
            space_id: SPACE.into(),
            source_id: source.clone(),
            security: Security::default(),
            subjects: vec![],
            subjects_state: SubjectsState::Complete,
        };
        let operation = StoredOperation {
            receipt: OperationReceipt {
                space_id: SPACE.into(),
                operation_id: head.operation_id.clone(),
                source_id: source.clone(),
                source_generation: head.generation.clone(),
                desired_revision: None,
                published_revision: None,
                state: OperationState::Erasing,
                accepted_at: intent.recorded_at,
                completed_at: None,
                error_code: None,
            },
            request_fingerprint: "original-request-fingerprint".into(),
            input_fingerprint: stable_key(&["input"]),
            authorization: authorization.clone(),
            contact_digest: None,
            producer_origin: SourceProducerOrigin::UnknownOriginal,
            predecessor_publication: None,
        };
        let values = [
            (
                control_thing("source_head", &head_key(SPACE, &source)),
                crate::source_publication::control_value(SPACE, &source, &head).unwrap(),
            ),
            (
                control_thing("source_authorization", &head_key(SPACE, &source)),
                crate::source_publication::control_value(SPACE, &source, &authorization).unwrap(),
            ),
            (
                control_thing("source_operation", &head.operation_id),
                crate::source_publication::control_value(SPACE, &source, &operation).unwrap(),
            ),
            (
                control_thing(
                    "source_identity",
                    &stable_key(&[SPACE, "external-original"]),
                ),
                crate::source_publication::control_value(
                    SPACE,
                    &source,
                    &SourceIdentity {
                        source_id: source.clone(),
                    },
                )
                .unwrap(),
            ),
            (
                control_thing(GUARD_TABLE, SPACE),
                object([
                    ("space_id", SPACE.into()),
                    ("journal_id", journal.identity().into()),
                    (
                        "key_fingerprint",
                        journal.reference_key_fingerprint().into(),
                    ),
                    ("version", 1_i64.into()),
                    ("membership_count", 1_i64.into()),
                    ("revision", 9_i64.into()),
                ]),
            ),
            (
                control_thing(MEMBERSHIP_TABLE, &stable_key(&[SPACE, &token])),
                object([("space_id", SPACE.into()), ("token", token.into())]),
            ),
        ];
        let rows = values
            .into_iter()
            .map(|(id, value)| native(id, value, 2, true))
            .collect();
        Self {
            directory,
            journal,
            intent,
            authority,
            rows,
        }
    }

    fn result(&self) -> StorageResult<BTreeSet<String>> {
        retained_history_control_commitments(
            &self.journal,
            &self.intent,
            &RecallHistorySnapshot::new(NS.into(), DB.into(), self.rows.clone()).unwrap(),
            &self.authority,
        )
    }
}

#[test]
fn exact_current_lifecycle_and_original_journal_controls_are_retained() {
    let fixture = Fixture::new();
    let expected = fixture
        .rows
        .iter()
        .map(|row| fixture.authority.version_commitment(row).unwrap())
        .collect::<BTreeSet<_>>();
    assert_eq!(fixture.result().unwrap(), expected);
    assert!(!fixture
        .directory
        .path()
        .join("history-commitments")
        .exists());
}

#[test]
fn historical_source_control_does_not_inherit_current_retention() {
    let mut fixture = Fixture::new();
    let mut previous = fixture.rows[0].clone();
    previous.version = 1;
    previous.timestamp = 1;
    previous.current = false;
    edit(&mut previous, |fields| {
        fields.insert("old_source_payload".into(), "erase-me".into());
    });
    let commitment = fixture.authority.version_commitment(&previous).unwrap();
    fixture.rows.push(previous);
    let retained = fixture.result().unwrap();
    assert_eq!(retained.len(), 6);
    assert!(!retained.contains(&commitment));
}

#[test]
fn unknown_fields_on_every_current_control_are_refused() {
    for index in 0..6 {
        let mut fixture = Fixture::new();
        edit(&mut fixture.rows[index], |fields| {
            fields.insert("hidden_source_text".into(), "payload-canary".into());
        });
        assert!(fixture.result().is_err(), "row {index}");
    }
}

#[test]
fn nested_payload_and_native_state_cannot_silently_drop_unknown_members() {
    for native_state in [false, true] {
        let mut fixture = Fixture::new();
        edit(&mut fixture.rows[1], |fields| {
            if native_state {
                let Value::Object(state) = fields.get_mut("state").unwrap() else {
                    panic!("state object");
                };
                state.insert("hidden_text".into(), "payload-canary".into());
            } else {
                let mut decoded: serde_json::Value =
                    serde_json::from_str(text(fields, "payload").unwrap()).unwrap();
                decoded["security"]["hidden_text"] = "payload-canary".into();
                fields.insert(
                    "payload".into(),
                    serde_json::to_string(&decoded).unwrap().into(),
                );
                fields.insert(
                    "state".into(),
                    crate::surreal::json_to_surreal(decoded).unwrap(),
                );
            }
        });
        assert!(fixture.result().is_err());
    }
}

#[test]
fn native_numeric_type_changes_are_not_equal_writer_states() {
    let mut fixture = Fixture::new();
    edit(&mut fixture.rows[1], |fields| {
        let Value::Object(state) = fields.get_mut("state").unwrap() else {
            panic!("state");
        };
        let Value::Object(security) = state.get_mut("security").unwrap() else {
            panic!("security");
        };
        security.insert("level".into(), Value::from(0.0_f64));
    });
    assert!(fixture.result().is_err());
}

#[test]
fn original_head_and_operation_read_nonces_are_checked_without_stripping() {
    let mut fixture = Fixture::new();
    for index in [0, 2] {
        edit(&mut fixture.rows[index], |fields| {
            fields.insert(
                "canonical_read_nonce".into(),
                uuid::Uuid::new_v4().to_string().into(),
            );
        });
    }
    assert_eq!(fixture.result().unwrap().len(), 6);
    edit(&mut fixture.rows[0], |fields| {
        fields.insert("canonical_read_nonce".into(), "payload-canary".into());
    });
    assert!(fixture.result().is_err());
    let mut fixture = Fixture::new();
    edit(&mut fixture.rows[1], |fields| {
        fields.insert(
            "canonical_read_nonce".into(),
            uuid::Uuid::new_v4().to_string().into(),
        );
    });
    assert!(fixture.result().is_err());
}

#[test]
fn wrong_physical_identity_or_original_intent_cannot_authorize_controls() {
    let mut fixture = Fixture::new();
    let wrong = control_thing("source_head", &uuid::Uuid::new_v4().to_string());
    fixture.rows[0].key =
        surrealdb::kvs::recall_history::record_key(NS, DB, &wrong.tb, &wrong.id).unwrap();
    edit(&mut fixture.rows[0], |fields| {
        fields.insert("id".into(), wrong.into());
    });
    assert!(fixture.result().is_err());
    let mut fixture = Fixture::new();
    fixture.intent.recorded_at += chrono::Duration::seconds(1);
    assert!(fixture.result().is_err());
}

#[test]
fn pending_source_payload_and_unsanitized_operation_refuse_retention() {
    for index in [0, 2] {
        let mut fixture = Fixture::new();
        edit(&mut fixture.rows[index], |fields| {
            if index == 0 {
                let mut head: SourceHead =
                    serde_json::from_str(text(fields, "payload").unwrap()).unwrap();
                head.desired_revision = Some("original-source-revision".into());
                fields.insert("payload".into(), encode_payload(&head).unwrap().into());
                fields.insert(
                    "state".into(),
                    crate::surreal::json_to_surreal(serde_json::to_value(head).unwrap()).unwrap(),
                );
            } else {
                let mut operation: StoredOperation =
                    serde_json::from_str(text(fields, "payload").unwrap()).unwrap();
                operation.receipt.error_code = Some("provider-source-error-canary".into());
                fields.insert("payload".into(), encode_payload(&operation).unwrap().into());
                fields.insert(
                    "state".into(),
                    crate::surreal::json_to_surreal(serde_json::to_value(operation).unwrap())
                        .unwrap(),
                );
            }
        });
        assert!(fixture.result().is_err());
    }
}

fn system_rows(fixture: &Fixture) -> Vec<(Thing, Value)> {
    vec![
        (
            control_thing("publication_schema", "canonical"),
            object([("version", 1_i64.into())]),
        ),
        (
            control_thing("recall_durability", "journal"),
            object([
                ("journal_id", fixture.journal.identity().into()),
                (
                    "reference_key_fingerprint",
                    fixture.journal.reference_key_fingerprint().into(),
                ),
            ]),
        ),
        (
            control_thing("native_read_fence", "doc:abc"),
            object([
                ("version", 1_i64.into()),
                ("nonce", uuid::Uuid::new_v4().to_string().into()),
            ]),
        ),
        (
            control_thing("migration_history", "tables"),
            object([
                ("name", "tables".into()),
                (
                    "checksum",
                    hex::encode(Sha256::digest(migration_sql("tables").unwrap().as_bytes())).into(),
                ),
                (
                    "applied_at",
                    Value::Datetime(fixture.intent.recorded_at.into()),
                ),
            ]),
        ),
    ]
}

#[test]
fn exact_historical_system_metadata_retains_each_original_native_version() {
    let mut fixture = Fixture::new();
    for (id, value) in system_rows(&fixture) {
        fixture
            .rows
            .push(native(id.clone(), value.clone(), 1, false));
        fixture.rows.push(native(id, value, 2, true));
    }
    let expected = fixture
        .rows
        .iter()
        .map(|row| fixture.authority.version_commitment(row).unwrap())
        .collect::<BTreeSet<_>>();
    assert_eq!(fixture.result().unwrap(), expected);
}

#[test]
fn semantic_head_migration_receipt_requires_exact_history_identity_and_checksum() {
    check_semantic_migration_receipt("source_head_semantic_epoch");
}

#[test]
fn canonical_read_migration_receipt_requires_exact_history_identity_and_checksum() {
    check_semantic_migration_receipt("canonical_read_semantic_epoch");
}

fn check_semantic_migration_receipt(name: &str) {
    for mode in 0..4 {
        let mut fixture = Fixture::new();
        let mut row = native(
            control_thing("migration_history", name),
            object([
                ("name", name.into()),
                (
                    "checksum",
                    hex::encode(Sha256::digest(migration_sql(name).unwrap().as_bytes())).into(),
                ),
                (
                    "applied_at",
                    Value::Datetime(fixture.intent.recorded_at.into()),
                ),
            ]),
            1,
            true,
        );
        edit(&mut row, |fields| match mode {
            1 => {
                fields.insert("checksum".into(), "0".repeat(64).into());
            }
            2 => {
                fields.insert("name".into(), "unknown-semantic-head-migration".into());
            }
            3 => {
                fields.insert("source_payload".into(), "must-not-survive".into());
            }
            _ => {}
        });
        let commitment = fixture.authority.version_commitment(&row).unwrap();
        fixture.rows.push(row);
        if mode == 0 {
            assert!(fixture.result().unwrap().contains(&commitment));
        } else {
            assert!(fixture.result().is_err());
        }
    }
}

#[test]
fn historical_system_payload_or_type_injection_is_refused() {
    for index in 0..4 {
        let mut fixture = Fixture::new();
        let (id, value) = system_rows(&fixture).remove(index);
        fixture
            .rows
            .push(native(id.clone(), value.clone(), 2, true));
        let mut previous = native(id, value, 1, false);
        edit(&mut previous, |fields| {
            fields.insert("source_payload".into(), "must-not-survive".into());
        });
        fixture.rows.push(previous);
        assert!(fixture.result().is_err());
    }
    let mut fixture = Fixture::new();
    fixture.rows.push(native(
        control_thing("publication_schema", "canonical"),
        object([("version", Value::from(1.0_f64))]),
        1,
        true,
    ));
    assert!(fixture.result().is_err());
}

#[test]
fn foreign_system_identity_checksum_and_native_datetime_are_refused() {
    for mode in 0..3 {
        let mut fixture = Fixture::new();
        let (id, value) = system_rows(&fixture).remove(3);
        let mut row = native(id, value, 1, true);
        edit(&mut row, |fields| match mode {
            0 => {
                fields.insert("name".into(), "unknown-migration".into());
            }
            1 => {
                fields.insert("checksum".into(), "0".repeat(64).into());
            }
            _ => {
                fields.insert(
                    "applied_at".into(),
                    fixture.intent.recorded_at.to_rfc3339().into(),
                );
            }
        });
        fixture.rows.push(row);
        assert!(fixture.result().is_err());
    }
}

#[test]
fn membership_loss_extra_token_or_count_change_refuses_complete_control_set() {
    for mode in 0..3 {
        let mut fixture = Fixture::new();
        match mode {
            0 => {
                fixture.rows.pop();
            }
            1 => edit(&mut fixture.rows[5], |fields| {
                fields.insert("token".into(), "a".repeat(64).into());
            }),
            _ => edit(&mut fixture.rows[4], |fields| {
                fields.insert("membership_count".into(), 2_i64.into());
            }),
        }
        assert!(fixture.result().is_err());
    }
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
async fn actual_native_consumer_and_producer_checkpoint_writers_are_retained() {
    let mut fixture = Fixture::new();
    let client = crate::StorageClient::new_memory().await.unwrap();
    crate::OutboxCheckpointRepository::new(client.clone())
        .advance(SPACE, "external-consumer", 0, 17)
        .await
        .unwrap();
    let mut sql = "BEGIN TRANSACTION;\n".to_owned();
    let mut bindings = BTreeMap::new();
    crate::event_sequence::append_reserve_sql(
        &mut sql,
        &mut bindings,
        SPACE,
        "retained_floor",
        17,
        2,
    )
    .unwrap();
    sql.push_str("COMMIT TRANSACTION;");
    client.query_bind_checked(&sql, bindings).await.unwrap();

    for (consumer, sequence) in [
        ("external-consumer", 17),
        (crate::event_sequence::RESERVED_CONSUMER, 19),
    ] {
        let id = control_thing(
            "outbox_checkpoint",
            &crate::outbox_checkpoint_repository::key(SPACE, consumer),
        );
        let mut response = client
            .query_bind_checked(
                "SELECT * FROM ONLY $id;",
                crate::bindings! { "id" => id.clone() },
            )
            .await
            .unwrap();
        let value: surrealdb::Value = response.take(0).unwrap();
        let value = value.into_inner();
        let Value::Object(fields) = &value else {
            panic!("actual persisted checkpoint required");
        };
        assert_eq!(fields.get("id"), Some(&Value::Thing(id.clone())));
        assert_eq!(integer(fields, "seq").unwrap(), sequence);
        // Retain the exact native postimage, including its datetime. Older
        // checkpoint versions receive no current-control exemption.
        let previous = native(id.clone(), value.clone(), 1, false);
        let previous_commitment = fixture.authority.version_commitment(&previous).unwrap();
        fixture.rows.push(previous);
        fixture.rows.push(native(id, value, 2, true));
        let retained = fixture.result().unwrap();
        assert!(!retained.contains(&previous_commitment));
    }
    assert_eq!(fixture.result().unwrap().len(), 8);
}

#[test]
fn checkpoint_retention_refuses_wrong_identity_and_non_writer_fields() {
    for consumer in [
        "external-consumer",
        crate::event_sequence::RESERVED_CONSUMER,
    ] {
        for mode in 0..8 {
            let mut fixture = Fixture::new();
            let id = control_thing(
                "outbox_checkpoint",
                &crate::outbox_checkpoint_repository::key(SPACE, consumer),
            );
            let mut row = native(
                id.clone(),
                object([
                    ("space_id", SPACE.into()),
                    ("consumer", consumer.into()),
                    ("seq", 17_i64.into()),
                    (
                        "updated_at",
                        Value::Datetime(fixture.intent.recorded_at.into()),
                    ),
                ]),
                2,
                true,
            );
            match mode {
                0 => {
                    // A valid identity belonging to another consumer cannot
                    // retain this consumer's otherwise valid checkpoint.
                    let wrong_consumer = format!("{consumer}-other");
                    let wrong = control_thing(
                        "outbox_checkpoint",
                        &crate::outbox_checkpoint_repository::key(SPACE, &wrong_consumer),
                    );
                    assert_ne!(id, wrong);
                    row.key =
                        surrealdb::kvs::recall_history::record_key(NS, DB, &wrong.tb, &wrong.id)
                            .unwrap();
                    edit(&mut row, |fields| {
                        fields.insert("id".into(), wrong.into());
                    });
                }
                _ => edit(&mut row, |fields| match mode {
                    1 => {
                        fields.remove("updated_at");
                    }
                    2 => {
                        fields.insert(
                            "updated_at".into(),
                            fixture.intent.recorded_at.to_rfc3339().into(),
                        );
                    }
                    3 => {
                        fields.insert("source_payload".into(), "must-not-survive".into());
                    }
                    4 => {
                        fields.insert("seq".into(), (-1_i64).into());
                    }
                    5 => {
                        fields.insert("seq".into(), Value::from(17.0_f64));
                    }
                    6 => {
                        fields.insert("consumer".into(), "".into());
                    }
                    _ => {
                        fields.insert("consumer".into(), "different-consumer".into());
                    }
                }),
            }
            fixture.rows.push(row);
            assert!(
                fixture.result().is_err(),
                "consumer {consumer}, mutation {mode}"
            );
        }
    }
}

#[test]
fn restart_recomputes_retention_without_durable_control_exemptions() {
    let fixture = Fixture::new();
    let original = fixture.result().unwrap();
    let reopened = ErasureJournal::open(fixture.directory.path()).unwrap();
    let snapshot = RecallHistorySnapshot::new(NS.into(), DB.into(), fixture.rows.clone()).unwrap();
    assert_eq!(
        retained_history_control_commitments(
            &reopened,
            &fixture.intent,
            &snapshot,
            &fixture.authority
        )
        .unwrap(),
        original
    );
    assert!(!fixture
        .directory
        .path()
        .join("history-commitments")
        .exists());
    let foreign = tempfile::tempdir().unwrap();
    let foreign_journal = ErasureJournal::open(foreign.path()).unwrap();
    assert!(retained_history_control_commitments(
        &foreign_journal,
        &fixture.intent,
        &snapshot,
        &fixture.authority
    )
    .is_err());
}

#[test]
fn original_snapshot_and_authority_are_required_even_for_well_formed_controls() {
    let fixture = Fixture::new();
    let mut snapshot =
        RecallHistorySnapshot::new(NS.into(), DB.into(), fixture.rows.clone()).unwrap();
    snapshot.records[0].timestamp += 1;
    assert!(retained_history_control_commitments(
        &fixture.journal,
        &fixture.intent,
        &snapshot,
        &fixture.authority
    )
    .is_err());
    let authority = RecallHistoryAuthority::new(
        NS.into(),
        DB.into(),
        fixture.journal.identity().into(),
        SPACE.into(),
        [17; 32],
        TYPED_UUID_TABLES
            .iter()
            .map(|table| (*table).into())
            .collect(),
        fixture
            .journal
            .reference_membership(&fixture.intent)
            .unwrap(),
    )
    .unwrap();
    let snapshot = RecallHistorySnapshot::new(NS.into(), DB.into(), fixture.rows.clone()).unwrap();
    assert!(retained_history_control_commitments(
        &fixture.journal,
        &fixture.intent,
        &snapshot,
        &authority
    )
    .is_err());
}

fn with_completion() -> Fixture {
    let mut fixture = Fixture::new();
    edit(&mut fixture.rows[0], |fields| {
        let mut head: SourceHead = serde_json::from_str(text(fields, "payload").unwrap()).unwrap();
        head.lifecycle = SourceVisibility::Erased;
        fields.insert("payload".into(), encode_payload(&head).unwrap().into());
        fields.insert(
            "state".into(),
            crate::surreal::json_to_surreal(serde_json::to_value(head).unwrap()).unwrap(),
        );
    });
    edit(&mut fixture.rows[2], |fields| {
        let mut operation: StoredOperation =
            serde_json::from_str(text(fields, "payload").unwrap()).unwrap();
        operation.receipt.state = OperationState::Erased;
        operation.receipt.completed_at = Some(operation.receipt.accepted_at);
        fields.insert("payload".into(), encode_payload(&operation).unwrap().into());
        fields.insert(
            "state".into(),
            crate::surreal::json_to_surreal(serde_json::to_value(operation).unwrap()).unwrap(),
        );
    });
    let Value::Object(fields) = fixture.rows[2].decoded_value().unwrap() else {
        panic!("operation");
    };
    let operation: StoredOperation =
        serde_json::from_str(text(&fields, "payload").unwrap()).unwrap();
    let receipt = &operation.receipt;
    let id = RecordId::new(
        TableName::SystemEvent,
        uuid::Uuid::new_v5(
            &uuid::Uuid::NAMESPACE_OID,
            format!("{}:source.erased", receipt.operation_id).as_bytes(),
        ),
    );
    let mut base = elephant_types::records::base::BaseFields::new(
        id.clone(),
        SPACE.into(),
        "source-lifecycle".into(),
    );
    base.security = operation.authorization.security;
    base.subjects = operation.authorization.subjects;
    base.subjects_state = operation.authorization.subjects_state;
    let event = elephant_types::records::SystemEvent {
        base,
        seq: 42,
        event_type: "source.erased".into(),
        payload: serde_json::json!({"space_id":SPACE,"source_id":receipt.source_id,"operation_id":receipt.operation_id,
            "records":[{"id":receipt.source_id,"table":"source","op":"delete"}]}),
    };
    fixture.rows.push(native(
        crate::surreal::thing_from_record_id(&id),
        crate::SystemEventRepository::system_event_to_value(&event).unwrap(),
        2,
        true,
    ));
    fixture
}

#[test]
fn completed_original_erasure_event_retains_only_exact_current_writer_value() {
    let mut fixture = with_completion();
    let current = fixture.rows.last().unwrap().clone();
    let proof = fixture.authority.version_commitment(&current).unwrap();
    assert!(fixture.result().unwrap().contains(&proof));
    let mut old = current;
    old.version = 1;
    old.timestamp = 1;
    old.current = false;
    let old_proof = fixture.authority.version_commitment(&old).unwrap();
    fixture.rows.push(old);
    assert!(!fixture.result().unwrap().contains(&old_proof));
}

#[test]
fn completion_label_cannot_exempt_payload_security_or_identity_changes() {
    for mode in 0..4 {
        let mut fixture = with_completion();
        edit(fixture.rows.last_mut().unwrap(), |fields| match mode {
            0 => {
                fields.insert("hidden_text".into(), "source-payload-canary".into());
            }
            1 => {
                let Value::Object(payload) = fields.get_mut("payload").unwrap() else {
                    panic!("payload");
                };
                payload.insert("hidden_text".into(), "source-payload-canary".into());
            }
            2 => {
                let Value::Object(security) = fields.get_mut("security").unwrap() else {
                    panic!("security");
                };
                security.insert("policy_id".into(), "substituted-policy".into());
            }
            _ => {
                fields.insert("created_by".into(), "external-producer".into());
            }
        });
        assert!(fixture.result().is_err());
    }
}

#[test]
fn completion_requires_actual_erased_operation_and_matching_source_record() {
    let mut fixture = with_completion();
    fixture.rows.remove(2);
    assert!(fixture.result().is_err());
    let mut fixture = with_completion();
    edit(fixture.rows.last_mut().unwrap(), |fields| {
        let Value::Object(payload) = fields.get_mut("payload").unwrap() else {
            panic!("payload");
        };
        payload.insert(
            "records".into(),
            Value::Array(surrealdb::sql::Array::default()),
        );
    });
    assert!(fixture.result().is_err());
}

#[path = "source_history_control_writer_tests.rs"]
mod original_writer_shapes;
