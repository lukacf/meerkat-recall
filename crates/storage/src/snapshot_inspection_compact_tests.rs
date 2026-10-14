//! Controls preserve exact row hashes while refusing expansion beyond bounds.
#![allow(clippy::unwrap_used, clippy::expect_used)]
use super::*;
use crate::snapshot::SnapshotManifest;
use crate::snapshot_inspection::{
    inspect_snapshot, validate_limits, InspectedSnapshotIdentity, SnapshotInspection,
};
use crate::StorageBackendKind;
use chrono::Utc;
use std::fs;

fn full_compact() -> SnapshotInspectionLimits {
    SnapshotInspectionLimits {
        include_records: true,
        compact_records: true,
        ..SnapshotInspectionLimits::default()
    }
}

fn expand(value: &mut NativeInspectionValue, blobs: &BTreeMap<String, InspectedSnapshotBlob>) {
    match value {
        NativeInspectionValue::StringRef(key) | NativeInspectionValue::BytesRef(key) => {
            let blob = &blobs[key.as_str()];
            let bytes = STANDARD_NO_PAD.decode(&blob.base64).unwrap();
            assert_eq!(bytes.len() as u64, blob.bytes);
            assert_eq!(crate::snapshot::digest(&bytes), *key);
            assert_eq!(STANDARD_NO_PAD.encode(&bytes), blob.base64);
            *value = if matches!(value, NativeInspectionValue::StringRef(_)) {
                NativeInspectionValue::String(String::from_utf8(bytes).unwrap())
            } else {
                NativeInspectionValue::Bytes(hex::encode(bytes))
            };
        }
        NativeInspectionValue::Array(values) => {
            for value in values {
                expand(value, blobs);
            }
        }
        NativeInspectionValue::Map(values) => {
            for value in values.values_mut() {
                expand(value, blobs);
            }
        }
        _ => {}
    }
}

#[test]
fn repeated_strings_and_bytes_share_one_blob_and_expand_to_exact_original_tags() {
    let text = "\u{0001}café".repeat(256);
    let original = NativeInspectionValue::Array(vec![
        NativeInspectionValue::String(text.clone()),
        NativeInspectionValue::Bytes(hex::encode(text.as_bytes())),
        NativeInspectionValue::String("small unchanged value".into()),
        NativeInspectionValue::Map(BTreeMap::from([(
            "nested".into(),
            NativeInspectionValue::String(text.clone()),
        )])),
    ]);
    let mut value = original.clone();
    let mut compactor = Compaction::default();
    compactor
        .record(&mut value, &full_compact(), &mut 0)
        .unwrap();
    assert_eq!(compactor.blobs.len(), 1);
    assert_eq!(compactor.unique_bytes, text.len());
    assert_eq!(
        compactor.logical_bytes,
        3 * text.len() + "small unchanged value".len()
    );
    let wire = serde_json::to_value(&value).unwrap();
    assert_eq!(wire["value"][0]["kind"], "string_ref");
    assert_eq!(wire["value"][1]["kind"], "bytes_ref");
    assert_eq!(wire["value"][0]["value"], wire["value"][1]["value"]);
    expand(&mut value, &compactor.blobs);
    assert_eq!(value, original);
}

#[test]
fn compaction_threshold_identities_and_every_occurrence_preserve_original_bounds() {
    let mut compactor = Compaction::default();
    let mut below = NativeInspectionValue::String("x".repeat(BLOB_THRESHOLD - 1));
    compactor
        .record(&mut below, &full_compact(), &mut 0)
        .unwrap();
    assert!(matches!(below, NativeInspectionValue::String(_)));
    let mut exact = NativeInspectionValue::String("x".repeat(BLOB_THRESHOLD));
    compactor
        .record(&mut exact, &full_compact(), &mut 0)
        .unwrap();
    assert!(matches!(exact, NativeInspectionValue::StringRef(_)));
    let identity = NativeInspectionValue::Thing(Box::new(InspectedSnapshotIdentity {
        table: "probe".into(),
        id: NativeInspectionValue::Array(vec![NativeInspectionValue::String("i".repeat(1024))]),
    }));
    let mut unchanged = identity.clone();
    compactor
        .record(&mut unchanged, &full_compact(), &mut 0)
        .unwrap();
    assert_eq!(identity, unchanged);
    assert_eq!(compactor.logical_bytes, 1023 + 1024 + 1024);
    assert_eq!(compactor.blobs.len(), 1);

    let limits = SnapshotInspectionLimits {
        payload_bytes: 2048,
        ..full_compact()
    };
    let mut compactor = Compaction::default();
    for _ in 0..2 {
        compactor
            .record(
                &mut NativeInspectionValue::String("a".repeat(1024)),
                &limits,
                &mut 0,
            )
            .unwrap();
    }
    assert_eq!(compactor.logical_bytes, 2048);
    assert_eq!(compactor.unique_bytes, 1024);
    assert!(compactor
        .record(
            &mut NativeInspectionValue::String("a".into()),
            &limits,
            &mut 0
        )
        .is_err());
    let mut compactor = Compaction::default();
    compactor.blob(&vec![b'a'; 2048], &limits, &mut 0).unwrap();
    assert_eq!(compactor.unique_bytes, 2048);
    assert!(compactor.blob(b"b", &limits, &mut 0).is_err());
    let mut compactor = Compaction::default();
    let limits = SnapshotInspectionLimits {
        string_bytes: 1024,
        ..full_compact()
    };
    compactor
        .record(
            &mut NativeInspectionValue::String("x".repeat(1024)),
            &limits,
            &mut 0,
        )
        .unwrap();
    assert!(compactor
        .record(
            &mut NativeInspectionValue::String("x".repeat(1025)),
            &limits,
            &mut 0
        )
        .is_err());
}

#[test]
fn compaction_refuses_collision_input_refs_report_overflow_and_missing_opt_in() {
    let bytes = "x".repeat(1024);
    let key = crate::snapshot::digest(bytes.as_bytes());
    let mut compactor = Compaction::default();
    compactor.blobs.insert(
        key,
        InspectedSnapshotBlob {
            bytes: 1024,
            base64: "wrong".into(),
        },
    );
    assert!(compactor
        .record(
            &mut NativeInspectionValue::String(bytes),
            &full_compact(),
            &mut 0
        )
        .is_err());
    let mut compactor = Compaction::default();
    for mut value in [
        NativeInspectionValue::StringRef("a".repeat(64)),
        NativeInspectionValue::BytesRef("a".repeat(64)),
    ] {
        assert!(compactor
            .record(&mut value, &full_compact(), &mut 0)
            .is_err());
    }
    let limits = SnapshotInspectionLimits {
        report_bytes: 100,
        ..full_compact()
    };
    assert!(compactor
        .record(
            &mut NativeInspectionValue::String("x".repeat(1024)),
            &limits,
            &mut 0
        )
        .is_err());
    assert!(validate_limits(&SnapshotInspectionLimits {
        compact_records: true,
        ..Default::default()
    })
    .is_err());
}

fn bundle(payload: &[u8], backend: StorageBackendKind) -> tempfile::TempDir {
    let root = tempfile::tempdir().unwrap();
    let now = Utc::now();
    let manifest = SnapshotManifest {
        version: 1,
        format: "recall-snapshot-v1".into(),
        snapshot_id: uuid::Uuid::new_v4().to_string(),
        journal_id: uuid::Uuid::new_v4().to_string(),
        backend,
        name: "compact control".into(),
        created_at: now,
        expires_at: now + chrono::Duration::hours(1),
        bytes: payload.len() as u64,
        sha256: crate::snapshot::digest(payload),
    };
    fs::write(
        root.path().join("manifest.json"),
        serde_json::to_vec(&manifest).unwrap(),
    )
    .unwrap();
    fs::write(root.path().join("data.snapshot"), payload).unwrap();
    root
}

fn assert_original_rows(original: &SnapshotInspection, compact: &SnapshotInspection) {
    assert_eq!(compact.format, "recall-snapshot-inspection-v2");
    assert_eq!(compact.row_count, original.row_count);
    assert_eq!(compact.table_counts, original.table_counts);
    assert_eq!(
        serde_json::to_value(&compact.schema_statements).unwrap(),
        serde_json::to_value(&original.schema_statements).unwrap()
    );
    for (before, after) in original.rows.iter().zip(&compact.rows) {
        assert_eq!(before.identity, after.identity);
        assert_eq!(before.sha256, after.sha256);
        assert_eq!(
            serde_json::to_value(&before.references).unwrap(),
            serde_json::to_value(&after.references).unwrap()
        );
        let mut expanded = after.record.clone().unwrap();
        expand(&mut expanded, compact.blobs.as_ref().unwrap());
        assert_eq!(before.record.as_ref(), Some(&expanded));
        assert_eq!(
            crate::snapshot::digest(&serde_json::to_vec(&expanded).unwrap()),
            after.sha256
        );
    }
}

#[test]
fn both_formats_keep_original_hashes_identities_schema_and_typed_references() {
    let text = "\u{0001}".repeat(2048);
    let embedded = serde_json::to_vec(&serde_json::json!({"backend":"elephant-db","version":1,"exported_at":"2026-01-01T00:00:00Z","tables":{"probe":[{"id":"probe:one","text":text,"duplicate":text}]}})).unwrap();
    let native_text = surrealdb::sql::Value::from(text.clone()).to_string();
    let native = format!("DEFINE TABLE probe SCHEMALESS; INSERT [{{id:probe:one,text:{native_text},duplicate:{native_text},ref:probe:two,bytes:encoding::base64::decode('{}')}}];", STANDARD_NO_PAD.encode(text.as_bytes()));
    for (backend, payload) in [
        (StorageBackendKind::ElephantDb, embedded),
        (StorageBackendKind::SurrealDb, native.into_bytes()),
    ] {
        let root = bundle(&payload, backend);
        let v1 = inspect_snapshot(
            root.path(),
            &SnapshotInspectionLimits {
                include_records: true,
                ..Default::default()
            },
        )
        .unwrap();
        assert!(v1.blobs.is_none());
        assert!(!serde_json::to_value(&v1)
            .unwrap()
            .as_object()
            .unwrap()
            .contains_key("blobs"));
        let v2 = inspect_snapshot(root.path(), &full_compact()).unwrap();
        assert_original_rows(&v1, &v2);
        assert_eq!(v2.blobs.as_ref().unwrap().len(), 1);
        assert_eq!(
            fs::read(root.path().join("data.snapshot")).unwrap(),
            payload
        );
    }
}
