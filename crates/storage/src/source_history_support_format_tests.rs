//! The historical boundary accepts the canonical writer's fingerprint contract.
use super::*;

fn with_fingerprint(
    mut row: RecallHistoryRecord,
    fingerprint: &str,
    projected: Option<&str>,
) -> RecallHistoryRecord {
    let Value::Object(mut fields) = row.decoded_value().unwrap() else {
        unreachable!()
    };
    let Some(Value::Strand(payload)) = fields.get("payload") else {
        unreachable!()
    };
    let mut support: SourceEffectSupport = serde_json::from_str(payload.as_str()).unwrap();
    support.record_fingerprint = fingerprint.into();
    fields.insert(
        "payload".into(),
        Value::from(serde_json::to_string(&support).unwrap()),
    );
    if let Some(projected) = projected {
        fields.insert("record_fingerprint".into(), Value::from(projected));
    }
    row.value = RecallHistoryRecord::encode_value(&Value::Object(fields)).unwrap();
    row
}

#[test]
#[expect(
    clippy::mutable_key_type,
    reason = "The oracle compares native keys without mutating them; regex cache internals do not change key identity."
)]
fn history_support_accepts_writer_uuidv5_with_exact_optional_projection() {
    let (_directory, _journal, intent, authority) = fixture();
    let record = entity();
    let fingerprint = record.fingerprint().unwrap();
    let original = support(
        &intent,
        &record,
        RecordId::new_v4(TableName::Source),
        None,
        12,
    );
    for projected in [None, Some(fingerprint.as_str())] {
        let row = with_fingerprint(original.clone(), &fingerprint, projected);
        let snapshot = RecallHistorySnapshot::new("ns".into(), "db".into(), vec![row]).unwrap();
        let boundaries = independent_boundaries(
            &records(&snapshot).unwrap(),
            &intent.space_id,
            Some(&intent.source_id),
            &BTreeSet::new(),
            &BTreeMap::new(),
            &authority,
        )
        .unwrap();
        assert_eq!(boundaries.len(), 1);
        let rows = &boundaries[&crate::surreal::thing_from_record_id(&record.base().id)];
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].version, 12);
        assert_eq!(rows[0].fingerprint, fingerprint);
    }
}

#[test]
fn history_support_refuses_wrong_fingerprint_format_and_projection() {
    let (_directory, _journal, intent, authority) = fixture();
    let record = entity();
    let fingerprint = record.fingerprint().unwrap();
    let original = support(
        &intent,
        &record,
        RecordId::new_v4(TableName::Source),
        None,
        12,
    );
    let format_example = uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_OID, b"support-format");
    let mut wrong_variant = *format_example.as_bytes();
    wrong_variant[8] &= 0x3f;
    let invalid_formats = [
        String::new(),
        "a".repeat(64),
        format_example.simple().to_string(),
        format_example.to_string().to_uppercase(),
        format_example.urn().to_string(),
        uuid::Uuid::new_v4().to_string(),
        uuid::Uuid::from_bytes(wrong_variant).to_string(),
        uuid::Uuid::nil().to_string(),
    ];
    let mut invalid_rows = invalid_formats
        .iter()
        .map(|invalid| with_fingerprint(original.clone(), invalid, Some(invalid)))
        .collect::<Vec<_>>();
    // Even two valid canonical UUIDv5 values must agree exactly across the
    // payload and optional native projection.
    let other = stable_key(&[&fingerprint, "different-projection"]);
    assert_ne!(other, fingerprint);
    invalid_rows.push(with_fingerprint(original, &fingerprint, Some(&other)));
    for row in invalid_rows {
        let snapshot = RecallHistorySnapshot::new("ns".into(), "db".into(), vec![row]).unwrap();
        assert!(independent_boundaries(
            &records(&snapshot).unwrap(),
            &intent.space_id,
            Some(&intent.source_id),
            &BTreeSet::new(),
            &BTreeMap::new(),
            &authority,
        )
        .is_err());
    }
}
