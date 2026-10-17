//! Historical wrapper compatibility, lexical custody and unchanged public guards.

use super::{historical_record_projection, import_batch, strict_json, TransferRecord};
use crate::assertion_repository::AssertionRepository;
use crate::{StorageClient, StorageResult};
use elephant_types::records::{
    Assertion, AssertionRetryState, BaseFields, EvidenceSpan, ResolutionStatus,
};
use elephant_types::{Link, RecordId, TableName};
use serde_json::{json, Value as JsonValue};
use surrealdb::sql::Value;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

fn assertion() -> TestResult<Assertion> {
    let mut base = BaseFields::new(
        RecordId::new_v4(TableName::Assertion),
        "literal-transfer".into(),
        "original-author".into(),
    );
    base.created_at = "2026-10-10T01:02:03.123456789Z".parse()?;
    base.updated_at = "2026-10-11T04:05:06.987654321Z".parse()?;
    let observed_at = base.created_at;
    let asserted_at = base.updated_at;
    Ok(Assertion {
        base,
        subject_text: "retained subject".into(),
        predicate_raw: "retained predicate".into(),
        predicate_id: None,
        object_text: None,
        object_literal: None,
        evidence: EvidenceSpan {
            revision: Link::new(RecordId::new_v4(TableName::DocRevision)),
            node: None,
            start: 0,
            end: 8,
            quote_hash: "a".repeat(64),
        },
        source_event_time: None,
        observed_at,
        asserted_at,
        assertion_key: uuid::Uuid::new_v4().to_string(),
        resolution_status: ResolutionStatus::Pending,
        resolution_method: None,
        resolution_confidence: None,
        retry_state: AssertionRetryState::default(),
        subject_entity: None,
        object_entity: None,
        confidence: 0.5,
        subject_entity_type: None,
        object_entity_type: None,
        significance: None,
        lifecycle_tier: "active".into(),
    })
}

fn wrappers() -> Vec<(&'static str, JsonValue)> {
    vec![
        ("string", json!("original å 🐾")),
        ("integer", json!(9_007_199_254_740_993_i64)),
        ("boolean", json!(false)),
        ("float", json!(1.25)),
        ("float", json!(1.0)),
        ("float", json!(-0.0)),
        ("json", json!([{"type":"string","value":"nested"}, null])),
    ]
}

#[test]
fn historical_wrappers_project_without_changing_original_or_public_guard() -> TestResult {
    for deleted in [false, true] {
        for (tag, literal) in wrappers() {
            let mut current = assertion()?;
            current.base.deleted_at = deleted.then_some(current.base.updated_at);
            current.object_literal = Some(literal.clone());
            let expected = serde_json::to_value(&current)?;
            let mut original = expected.clone();
            original["object_literal"] = json!({"type":tag,"value":literal});
            let raw = serde_json::to_string_pretty(&original)?;
            let original_bytes = serde_json::to_vec(&original)?;
            let projected = historical_record_projection("assertion", &original)?;
            assert_eq!(
                serde_json::to_vec(&projected)?,
                serde_json::to_vec(&expected)?
            );
            assert_eq!(serde_json::to_vec(&original)?, original_bytes);
            let transferred = TransferRecord::from_json("assertion", &raw)?;
            assert_eq!(
                serde_json::to_vec(&transferred.json()?)?,
                serde_json::to_vec(&expected)?
            );
            assert_eq!(transferred.id(), &current.base.id);
            assert_eq!(raw, serde_json::to_string_pretty(&original)?);
            let Value::Object(native) = transferred.native()? else {
                return Err("assertion native payload was not an object".into());
            };
            assert_eq!(
                native.get("object_literal"),
                Some(&crate::surreal::json_to_surreal(
                    original["object_literal"].clone()
                )?)
            );
            current.object_literal = Some(original["object_literal"].clone());
            assert!(AssertionRepository::assertion_to_value(&current).is_err());
        }
    }
    Ok(())
}

#[test]
fn historical_wrapper_type_mismatch_and_original_field_loss_still_refuse() -> TestResult {
    let mut original = serde_json::to_value(assertion()?)?;
    for (tag, literal) in [
        ("integer", json!(1.0)),
        ("integer", json!("1")),
        ("float", json!(1)),
        ("boolean", json!(0)),
        ("string", json!(false)),
        ("json", json!({"retained":true})),
        ("json", JsonValue::Null),
    ] {
        original["object_literal"] = json!({"type":tag,"value":literal});
        assert!(historical_record_projection("assertion", &original).is_err());
        assert!(TransferRecord::from_json("assertion", &original.to_string()).is_err());
    }
    original["object_literal"] = json!({"type":"string","value":"valid"});
    original["unknown_original_field"] = true.into();
    assert!(TransferRecord::from_json("assertion", &original.to_string()).is_err());
    for raw in [
        r#"{"object_literal":{"type":"string","type":"integer","value":1}}"#,
        r#"{"object_literal":{"type":"integer","value":18446744073709551616}}"#,
        r#"{"object_literal":{"type":"string","value":"\u0000"}}"#,
    ] {
        assert!(strict_json(raw).is_err());
        assert!(TransferRecord::from_json("assertion", raw).is_err());
    }
    Ok(())
}

#[test]
fn historical_projection_preserves_ordinary_objects_tables_and_numeric_bits() -> TestResult {
    for literal in [
        json!({"type":"unknown","value":4}),
        json!({"type":"string","value":"data","extra":true}),
        json!({"type":"string"}),
        json!({"type":4,"value":7}),
        json!({"nested":{"type":"string","value":"data"}}),
        json!("already public"),
    ] {
        let mut original = serde_json::to_value(assertion()?)?;
        original["object_literal"] = literal;
        let bytes = serde_json::to_vec(&original)?;
        assert_eq!(
            serde_json::to_vec(&historical_record_projection("assertion", &original)?)?,
            bytes
        );
    }
    let other = json!({"object_literal":{"type":"string","value":"retained"}});
    assert_eq!(historical_record_projection("attribute", &other)?, other);
    for raw in [
        r#"{"object_literal":{"type":"integer","value":18446744073709551615}}"#,
        r#"{"object_literal":{"type":"float","value":1.0}}"#,
        r#"{"object_literal":{"type":"float","value":-0.0}}"#,
    ] {
        let original = strict_json(raw)?;
        let projected = historical_record_projection("assertion", &original)?;
        assert_eq!(
            serde_json::to_vec(&projected["object_literal"])?,
            serde_json::to_vec(&original["object_literal"]["value"])?
        );
    }
    Ok(())
}

async fn verify_native_literals(
    client: &StorageClient,
    rows: &[TransferRecord],
) -> StorageResult<()> {
    for row in rows {
        let Value::Object(expected) = row.native()? else {
            return Err(super::invalid("assertion native payload was not an object"));
        };
        let mut response = client
            .query_bind_checked(
                "SELECT VALUE object_literal FROM $id",
                crate::bindings! {"id" => crate::surreal::thing_from_record_id(row.id())},
            )
            .await?;
        let raw: surrealdb::Value = response.take(0)?;
        let Value::Array(actual) = raw.into_inner() else {
            return Err(super::invalid("native literal readback was not an array"));
        };
        assert_eq!(actual.len(), 1);
        assert_eq!(actual.first(), expected.get("object_literal"));
    }
    Ok(())
}

#[tokio::test]
async fn historical_literal_native_transfer_retains_wrappers_and_typed_readback() -> TestResult {
    let mut rows = Vec::new();
    for deleted in [false, true] {
        for (tag, literal) in wrappers() {
            let mut item = assertion()?;
            item.base.deleted_at = deleted.then_some(item.base.updated_at);
            let mut original = serde_json::to_value(item)?;
            original["object_literal"] = json!({"type":tag,"value":literal});
            rows.push(TransferRecord::from_json(
                "assertion",
                &original.to_string(),
            )?);
        }
    }
    let client = StorageClient::new_memory().await?;
    let digest = "a".repeat(64);
    assert!(import_batch(&client, &digest, &rows).await.is_err());
    let bound = digest.clone();
    Box::pin(
        client.with_transfer_barrier(&digest, move |exclusive| async move {
            assert!(import_batch(&exclusive, &"b".repeat(64), &rows)
                .await
                .is_err());
            import_batch(&exclusive, &bound, &rows).await?;
            verify_native_literals(&exclusive, &rows).await?;
            exclusive.initialize_journal_identity().await
        }),
    )
    .await?;
    client
        .close_owned_command_connection(30_000)
        .await
        .map_err(Into::into)
}
