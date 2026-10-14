//! RCT representation contract tests.
//!
//! These tests are executed by the `cargo test rct_contracts --workspace` gate and must:
//! - match the `rct_contracts` filter (module/test names include `rct_contracts`)
//! - validate key representation boundaries (JSON <-> DB <-> JSON)

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::doc_markdown,
    unused_imports,
    dead_code
)]

use chrono::Utc;
use std::sync::atomic::{AtomicU64, Ordering};

use serde::Deserialize;

use elephant_types::records::assertion::{
    Assertion, AssertionRetryState, EvidenceSpan, ResolutionStatus,
};
use elephant_types::records::base::{BaseFields, Provenance};
use elephant_types::records::entity::Entity;
use elephant_types::{Link, RecordId, TableName};

use crate::client::StorageClient;
use crate::StorageConfig;
use crate::{AssertionRepository, Repository};

static TEST_DB_COUNTER: AtomicU64 = AtomicU64::new(91000);

async fn setup_client_and_schema() -> StorageClient {
    let db_id = TEST_DB_COUNTER.fetch_add(1, Ordering::SeqCst);
    let config = StorageConfig {
        database: format!("rct_contracts_test_{db_id}"),
        ..StorageConfig::default()
    };
    let client = StorageClient::new(config).await.unwrap();

    client
        .query(
            r"
            DEFINE TABLE assertion SCHEMALESS;
        ",
        )
        .await
        .unwrap();

    client
}

fn assert_json_object_omits_keys(json: &serde_json::Value, path: &str, forbidden_keys: &[&str]) {
    let obj = json
        .as_object()
        .unwrap_or_else(|| panic!("{path} must be a JSON object"));
    for key in forbidden_keys {
        assert!(
            !obj.contains_key(*key),
            "{path} must omit optional field {key} when NONE"
        );
    }
}

#[test]
fn rct_contracts_provenance_omits_none_fields_in_json() {
    let provenance = Provenance::default();
    let json = serde_json::to_value(&provenance).unwrap();

    assert_json_object_omits_keys(
        &json,
        "provenance",
        &[
            "extractor",
            "work_run_id",
            "run_id",
            "prompt_id",
            "config_id",
        ],
    );
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep client, repo in the original fixture scope through assertions and cleanup."
)]
async fn rct_contracts_provenance_omits_none_fields_after_db_round_trip() {
    let client = setup_client_and_schema().await;
    let repo = AssertionRepository::new(client);

    let id = RecordId::new_v4(TableName::Assertion);
    let base = BaseFields::new(
        id.clone(),
        "test_space".to_string(),
        "test_agent".to_string(),
    );

    let doc_revision_id = RecordId::new_v4(TableName::DocRevision);
    let now = Utc::now();

    let assertion = Assertion {
        base,
        subject_text: "John Doe".to_string(),
        predicate_raw: "works_at".to_string(),
        predicate_id: None,
        object_text: Some("Acme Corp".to_string()),
        object_literal: None,
        evidence: EvidenceSpan {
            revision: Link::new(doc_revision_id),
            node: None,
            start: 0,
            end: 10,
            quote_hash: "sha256:rct_contracts".to_string(),
        },
        source_event_time: None,
        observed_at: now,
        asserted_at: now,
        assertion_key: "rct_contracts_key".to_string(),
        resolution_status: ResolutionStatus::Pending,
        resolution_method: None,
        resolution_confidence: None,
        retry_state: AssertionRetryState::default(),
        subject_entity: None,
        object_entity: None,
        confidence: 0.9,
        subject_entity_type: None,
        object_entity_type: None,
        significance: None,
        lifecycle_tier: "active".to_string(),
    };

    let created = repo.create(assertion).await.unwrap();
    let json = serde_json::to_value(&created).unwrap();

    let provenance_json = json
        .get("provenance")
        .unwrap_or_else(|| panic!("assertion JSON must include provenance"));

    assert_json_object_omits_keys(
        provenance_json,
        "assertion.provenance",
        &[
            "extractor",
            "work_run_id",
            "run_id",
            "prompt_id",
            "config_id",
        ],
    );
}

fn make_assertion_with_object_literal(object_literal: Option<serde_json::Value>) -> Assertion {
    let id = RecordId::new_v4(TableName::Assertion);
    let base = BaseFields::new(id, "test_space".to_string(), "test_agent".to_string());

    let doc_revision_id = RecordId::new_v4(TableName::DocRevision);
    let now = Utc::now();

    Assertion {
        base,
        subject_text: "Alice".to_string(),
        predicate_raw: "has_attribute".to_string(),
        predicate_id: None,
        object_text: None,
        object_literal,
        evidence: EvidenceSpan {
            revision: Link::new(doc_revision_id),
            node: None,
            start: 0,
            end: 10,
            quote_hash: "sha256:rct_contracts_object_literal".to_string(),
        },
        source_event_time: None,
        observed_at: now,
        asserted_at: now,
        assertion_key: format!("rct_contracts_object_literal_{}", uuid::Uuid::new_v4()),
        resolution_status: ResolutionStatus::Pending,
        resolution_method: None,
        resolution_confidence: None,
        retry_state: AssertionRetryState::default(),
        subject_entity: None,
        object_entity: None,
        confidence: 0.9,
        subject_entity_type: None,
        object_entity_type: None,
        significance: None,
        lifecycle_tier: "active".to_string(),
    }
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep client, repo in the original fixture scope through assertions and cleanup."
)]
async fn rct_contracts_assertion_object_literal_round_trips_common_json_shapes() {
    let client = setup_client_and_schema().await;
    let repo = AssertionRepository::new(client);

    let cases = [
        serde_json::json!(42),
        serde_json::json!(std::f64::consts::PI),
        serde_json::json!(true),
        serde_json::json!("hello"),
        serde_json::json!([1, 2, 3]),
        serde_json::json!({"a": 1, "b": "two"}),
    ];

    for value in cases {
        let assertion = make_assertion_with_object_literal(Some(value.clone()));
        let created = repo.create(assertion).await.unwrap();
        assert_eq!(created.object_literal, Some(value));
    }
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep client, repo in the original fixture scope through assertions and cleanup."
)]
async fn rct_contracts_assertion_object_literal_rejects_reserved_wrapper_object_shape() {
    let client = setup_client_and_schema().await;
    let repo = AssertionRepository::new(client);

    let reserved = serde_json::json!({ "type": "integer", "value": 30 });
    let assertion = make_assertion_with_object_literal(Some(reserved));
    let err = repo
        .create(assertion)
        .await
        .expect_err("reserved shape must error");
    assert!(matches!(
        err,
        crate::error::StorageError::SerializationError(_)
    ));
}

#[test]
fn rct_contracts_record_id_deserializes_surreal_uuid_map_shape() {
    let uuid = uuid::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
    let id = RecordId::new(TableName::Entity, uuid);
    let thing = crate::surreal::thing_from_record_id(&id);

    let json = serde_json::to_value(&thing).unwrap();
    let parsed: RecordId = serde_json::from_value(json).unwrap();

    assert_eq!(parsed, id);
}

#[test]
fn rct_contracts_link_deserializes_surreal_uuid_map_shape() {
    let uuid = uuid::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
    let id = RecordId::new(TableName::Entity, uuid);
    let thing = crate::surreal::thing_from_record_id(&id);

    let json = serde_json::to_value(&thing).unwrap();
    let parsed: Link<Entity> = serde_json::from_value(json).unwrap();

    assert_eq!(parsed.id(), &id);
}

#[test]
fn rct_contracts_record_id_deserializes_uuid_byte_seq_shape() {
    let uuid = uuid::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
    let expected = RecordId::new(TableName::Entity, uuid);

    let json = serde_json::json!({
        "tb": "entity",
        "id": uuid.as_bytes().to_vec(),
    });

    let parsed: RecordId = serde_json::from_value(json).unwrap();
    assert_eq!(parsed, expected);
}

#[test]
fn rct_contracts_link_deserializes_uuid_byte_seq_shape() {
    let uuid = uuid::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
    let expected = RecordId::new(TableName::Entity, uuid);

    let json = serde_json::json!({
        "tb": "entity",
        "id": uuid.as_bytes().to_vec(),
    });

    let parsed: Link<Entity> = serde_json::from_value(json).unwrap();
    assert_eq!(parsed.id(), &expected);
}

fn deserialize_surreal_thing_with_id_bytes<T>(table: &str, id: &[u8]) -> T
where
    T: for<'de> Deserialize<'de>,
{
    use serde::de::value::{BorrowedBytesDeserializer, Error as ValueError};
    use serde::de::{self, DeserializeSeed, IntoDeserializer, MapAccess, Visitor};

    struct ThingBytes<'de> {
        table: &'de str,
        id: &'de [u8],
    }

    struct ThingBytesMapAccess<'de> {
        table: &'de str,
        id: &'de [u8],
        idx: u8,
    }

    impl<'de> MapAccess<'de> for ThingBytesMapAccess<'de> {
        type Error = ValueError;

        fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>, Self::Error>
        where
            K: DeserializeSeed<'de>,
        {
            match self.idx {
                0 => seed.deserialize("tb".into_deserializer()).map(Some),
                1 => seed.deserialize("id".into_deserializer()).map(Some),
                _ => Ok(None),
            }
        }

        fn next_value_seed<V>(&mut self, seed: V) -> Result<V::Value, Self::Error>
        where
            V: DeserializeSeed<'de>,
        {
            let out = match self.idx {
                0 => seed.deserialize(self.table.into_deserializer())?,
                1 => seed.deserialize(BorrowedBytesDeserializer::new(self.id))?,
                _ => return Err(de::Error::custom("no more map values")),
            };
            self.idx += 1;
            Ok(out)
        }
    }

    impl<'de> de::Deserializer<'de> for ThingBytes<'de> {
        type Error = ValueError;

        fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
        where
            V: Visitor<'de>,
        {
            visitor.visit_map(ThingBytesMapAccess {
                table: self.table,
                id: self.id,
                idx: 0,
            })
        }

        serde::forward_to_deserialize_any! {
            bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string bytes byte_buf
            option unit unit_struct newtype_struct seq tuple tuple_struct map struct enum identifier
            ignored_any
        }
    }

    T::deserialize(ThingBytes { table, id }).unwrap()
}

#[test]
fn rct_contracts_record_id_deserializes_uuid_bytes_shape() {
    let uuid = uuid::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
    let expected = RecordId::new(TableName::Entity, uuid);

    let parsed: RecordId = deserialize_surreal_thing_with_id_bytes("entity", uuid.as_bytes());
    assert_eq!(parsed, expected);
}

#[test]
fn rct_contracts_link_deserializes_uuid_bytes_shape() {
    let uuid = uuid::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
    let expected = RecordId::new(TableName::Entity, uuid);

    let parsed: Link<Entity> = deserialize_surreal_thing_with_id_bytes("entity", uuid.as_bytes());
    assert_eq!(parsed.id(), &expected);
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep client in the original fixture scope through assertions and cleanup."
)]
async fn rct_contracts_record_id_json_db_json_round_trip_preserves_string_format() {
    #[derive(serde::Deserialize)]
    struct Row {
        target: RecordId,
    }

    let client = setup_client_and_schema().await;
    client
        .query("DEFINE TABLE rct_contracts_id_roundtrip SCHEMALESS;")
        .await
        .unwrap();

    let json = serde_json::Value::String("entity:550e8400-e29b-41d4-a716-446655440000".to_string());
    let original: RecordId = serde_json::from_value(json.clone()).unwrap();

    client
        .query_bind(
            "CREATE rct_contracts_id_roundtrip CONTENT { target: $target };",
            crate::bindings! {
                "target" => crate::surreal::thing_from_record_id(&original),
            },
        )
        .await
        .unwrap();

    let mut response = client
        .query("SELECT target FROM rct_contracts_id_roundtrip;")
        .await
        .unwrap();
    let rows: Vec<Row> = response.take(0).unwrap();
    let row = rows.into_iter().next().unwrap();

    assert_eq!(row.target, original);
    assert_eq!(serde_json::to_value(&row.target).unwrap(), json);
}
