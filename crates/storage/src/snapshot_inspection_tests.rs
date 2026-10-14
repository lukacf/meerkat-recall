//! Offline parsing cases and real supported-export fixture coverage.
#![allow(clippy::expect_used, clippy::unwrap_used)]
use super::*;

fn bundle(payload: &[u8], backend: StorageBackendKind) -> tempfile::TempDir {
    let directory = tempfile::tempdir().unwrap();
    let now = Utc::now();
    let manifest = SnapshotManifest {
        version: 1,
        format: "recall-snapshot-v1".into(),
        snapshot_id: uuid::Uuid::new_v4().to_string(),
        journal_id: uuid::Uuid::new_v4().to_string(),
        backend,
        name: "offline fixture".into(),
        created_at: now,
        expires_at: now + chrono::Duration::hours(1),
        bytes: payload.len() as u64,
        sha256: crate::snapshot::digest(payload),
    };
    fs::write(
        directory.path().join("manifest.json"),
        serde_json::to_vec(&manifest).unwrap(),
    )
    .unwrap();
    fs::write(directory.path().join("data.snapshot"), payload).unwrap();
    directory
}

fn full() -> SnapshotInspectionLimits {
    SnapshotInspectionLimits {
        include_records: true,
        ..SnapshotInspectionLimits::default()
    }
}

fn refused(directory: &Path, limits: &SnapshotInspectionLimits) {
    let error = inspect_snapshot(directory, limits).unwrap_err();
    assert!(
        matches!(error, StorageError::DataIntegrityError(ref message) if message.starts_with("snapshot_inspection_incomplete:"))
    );
}

#[tokio::test]
#[expect(
    clippy::literal_string_with_formatting_args,
    reason = "These braces are literal SurrealQL object syntax in the original query fixture."
)]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
async fn actual_named_exports_are_inspected_offline_without_mutation() {
    for backend in [
        StorageBackendKind::ElephantDb,
        StorageBackendKind::SurrealDb,
    ] {
        let root = tempfile::tempdir().unwrap();
        let storage = crate::StorageClient::new_with_durability(
            crate::StorageConfig {
                backend,
                ..Default::default()
            },
            None,
            Some(root.path().join("journal")),
        )
        .await
        .unwrap();
        if let Some(store) = storage.embedded_store() {
            store.mutate(|tx| Ok(tx.upsert_extension_row("inspection_probe", serde_json::json!({
                "id":"inspection_probe:first", "space_id":"scope", "secret":"private-inspection-canary", "nested":[{"kept":true}],
            }))?)).await.unwrap();
        } else {
            storage.query_checked("CREATE inspection_probe:first SET space_id='scope', secret='private-inspection-canary', nested=[{kept:true}]").await.unwrap();
        }
        let output = root.path().join("bundle");
        let manifest = crate::snapshot::export_named(
            &storage,
            &output,
            "actual-export",
            &crate::snapshot::SnapshotRetention::default(),
        )
        .await
        .unwrap();
        let original_payload = fs::read(output.join("data.snapshot")).unwrap();
        let original_manifest = fs::read(output.join("manifest.json")).unwrap();
        let report = inspect_snapshot(&output, &SnapshotInspectionLimits::default()).unwrap();
        assert_eq!(report.manifest.snapshot_id, manifest.snapshot_id);
        assert_eq!(report.table_counts["inspection_probe"], 1);
        assert!(report.rows.iter().all(|row| row.record.is_none()));
        assert!(!serde_json::to_string(&report)
            .unwrap()
            .contains("private-inspection-canary"));
        let report = inspect_snapshot(&output, &full()).unwrap();
        assert!(serde_json::to_string(&report)
            .unwrap()
            .contains("private-inspection-canary"));
        assert_eq!(
            fs::read(output.join("data.snapshot")).unwrap(),
            original_payload
        );
        assert_eq!(
            fs::read(output.join("manifest.json")).unwrap(),
            original_manifest
        );
        assert_eq!(
            fs::read_dir(&output).unwrap().count(),
            if backend == StorageBackendKind::SurrealDb {
                3
            } else {
                2
            }
        );
    }
}

#[test]
fn native_ids_and_nested_references_retain_types_without_string_guessing() {
    let input = br"OPTION IMPORT;
DEFINE TABLE probe SCHEMALESS;
INSERT [
 {id:probe:7, space_id:'scope', value:'probe:7'},
 {id:probe:`7`, space_id:'scope', value:[probe:7, {nested:probe:u'08a48b2d-c559-4c57-a8be-32e1ab5c77d4'}]},
 {id:probe:u'08a48b2d-c559-4c57-a8be-32e1ab5c77d4', space_id:'scope', scalar:u'08a48b2d-c559-4c57-a8be-32e1ab5c77d4', bytes:encoding::base64::decode('AQID'), decimal:1.230dec},
 {id:probe:['composite', 2], space_id:'scope', value:NONE}
];";
    let directory = bundle(input, StorageBackendKind::SurrealDb);
    let report = inspect_snapshot(directory.path(), &full()).unwrap();
    assert_eq!(report.row_count, 4);
    assert_eq!(report.statement_count, 3);
    assert_eq!(report.rows[0].identity.id, NativeInspectionValue::Int(7));
    assert_eq!(
        report.rows[1].identity.id,
        NativeInspectionValue::String("7".into())
    );
    assert!(matches!(
        report.rows[2].identity.id,
        NativeInspectionValue::Uuid(_)
    ));
    assert!(matches!(
        report.rows[3].identity.id,
        NativeInspectionValue::Array(_)
    ));
    assert_eq!(
        report.rows[0].references.len(),
        1,
        "only physical native id, never string value"
    );
    assert_eq!(report.rows[1].references.len(), 3);
    assert!(report.rows[1]
        .references
        .iter()
        .any(|reference| reference.path == "/value/1/nested"));
    assert!(report.schema_statements[0]
        .statement
        .as_ref()
        .unwrap()
        .contains("DEFINE TABLE"));
}

#[test]
fn duplicate_identities_and_duplicate_keys_are_incomplete() {
    for input in [
        "INSERT [{id:p:1, x:1}, {id:p:1, x:2}];",
        "INSERT [{id:p:1, x:1, x:2}];",
        "INSERT [{id:p:1, nested:{x:1, \"x\":2}}];",
        "INSERT [{id:p:1, nested:{\"x\":1, '\\u{78}':2}}];",
    ] {
        let directory = bundle(input.as_bytes(), StorageBackendKind::SurrealDb);
        refused(directory.path(), &full());
    }
    let directory = bundle(br#"{"backend":"elephant-db","version":1,"exported_at":"2026-01-01T00:00:00Z","tables":{"p":[{"id":"p:one","x":1,"x":2}]}}"#, StorageBackendKind::ElephantDb);
    refused(directory.path(), &full());
}

#[test]
fn unknown_statements_and_expressions_never_execute_or_partially_succeed() {
    for input in [
        "INSERT [{id:p:1}]; DELETE p;",
        "INSERT [{id:p:1, value:rand::uuid()}];",
        "INSERT [{id:p:1, value:(SELECT * FROM p)}];",
        "INSERT INTO p [{id:p:1}];",
        "BEGIN; INSERT [{id:p:1}]; COMMIT;",
        "OPTION FIELDS;",
    ] {
        let directory = bundle(input.as_bytes(), StorageBackendKind::SurrealDb);
        refused(directory.path(), &full());
    }
}

#[test]
fn file_manifest_payload_and_all_work_bounds_fail_closed() {
    let directory = bundle(
        b"INSERT [{id:p:1, space_id:'scope', v:'abcdef', refs:[p:2,p:3]}];",
        StorageBackendKind::SurrealDb,
    );
    for limits in [
        SnapshotInspectionLimits {
            payload_bytes: 5,
            ..full()
        },
        SnapshotInspectionLimits {
            manifest_bytes: 5,
            ..full()
        },
        SnapshotInspectionLimits {
            string_bytes: 3,
            ..full()
        },
        SnapshotInspectionLimits { nodes: 3, ..full() },
        SnapshotInspectionLimits { depth: 1, ..full() },
        SnapshotInspectionLimits {
            references: 1,
            ..full()
        },
        SnapshotInspectionLimits {
            report_bytes: 40,
            ..full()
        },
        SnapshotInspectionLimits { rows: 0, ..full() },
        SnapshotInspectionLimits {
            depth: 129,
            ..full()
        },
    ] {
        refused(directory.path(), &limits);
    }
    let table_limit = bundle(
        b"DEFINE TABLE a; DEFINE TABLE b;",
        StorageBackendKind::SurrealDb,
    );
    refused(
        table_limit.path(),
        &SnapshotInspectionLimits {
            tables: 1,
            ..full()
        },
    );
    refused(
        table_limit.path(),
        &SnapshotInspectionLimits {
            statements: 1,
            ..full()
        },
    );
    let row_limit = bundle(
        b"INSERT [{id:p:1},{id:p:2}];",
        StorageBackendKind::SurrealDb,
    );
    refused(
        row_limit.path(),
        &SnapshotInspectionLimits { rows: 1, ..full() },
    );
    fs::write(directory.path().join("data.snapshot"), b"changed").unwrap();
    refused(directory.path(), &full());
    let manifest_path = row_limit.path().join("manifest.json");
    let mut manifest: SnapshotManifest =
        serde_json::from_slice(&fs::read(&manifest_path).unwrap()).unwrap();
    manifest.expires_at = Utc::now() - chrono::Duration::seconds(1);
    fs::write(&manifest_path, serde_json::to_vec(&manifest).unwrap()).unwrap();
    refused(row_limit.path(), &full());
}

#[cfg(unix)]
#[test]
fn symlink_bundle_and_payload_are_refused() {
    use std::os::unix::fs::symlink;
    let directory = bundle(b"INSERT [{id:p:1}];", StorageBackendKind::SurrealDb);
    let other = tempfile::tempdir().unwrap();
    symlink(directory.path(), other.path().join("link")).unwrap();
    refused(&other.path().join("link"), &full());
    let payload = directory.path().join("data.snapshot");
    fs::rename(&payload, other.path().join("payload")).unwrap();
    symlink(other.path().join("payload"), &payload).unwrap();
    refused(directory.path(), &full());
}

#[test]
fn unknown_manifest_and_embedded_shapes_are_incomplete() {
    for payload in [
        br#"{"backend":"other","version":1,"exported_at":"2026-01-01T00:00:00Z","tables":{}}"#.as_slice(),
        br#"{"backend":"elephant-db","version":2,"exported_at":"2026-01-01T00:00:00Z","tables":{}}"#,
        br#"{"backend":"elephant-db","version":1,"exported_at":"2026-01-01T00:00:00Z","tables":{"p":[{"id":"p:one"},{"id":"p:one"}]}}"#,
        br#"{"backend":"elephant-db","version":1,"exported_at":"2026-01-01T00:00:00Z","tables":{"p":[{"id":{"table":"q","id":"one"}}]}}"#,
    ] {
        let directory = bundle(payload, StorageBackendKind::ElephantDb);
        refused(directory.path(), &full());
    }
    let directory = bundle(b"INSERT [{id:p:1}];", StorageBackendKind::SurrealDb);
    let manifest_path = directory.path().join("manifest.json");
    let mut manifest: SnapshotManifest =
        serde_json::from_slice(&fs::read(&manifest_path).unwrap()).unwrap();
    manifest.format = "unknown".into();
    fs::write(manifest_path, serde_json::to_vec(&manifest).unwrap()).unwrap();
    refused(directory.path(), &full());
}

#[test]
fn embedded_row_and_table_limits_apply_during_decode() {
    let directory = bundle(br#"{"backend":"elephant-db","version":1,"exported_at":"2026-01-01T00:00:00Z","tables":{"p":[{"id":"p:one"},{"id":"p:two"}],"q":[]}}"#, StorageBackendKind::ElephantDb);
    refused(
        directory.path(),
        &SnapshotInspectionLimits { rows: 1, ..full() },
    );
    refused(
        directory.path(),
        &SnapshotInspectionLimits {
            tables: 1,
            ..full()
        },
    );
}

fn embedded_string_row(text: &str) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({
        "backend":"elephant-db", "version":1,
        "exported_at":"2026-01-01T00:00:00Z",
        "tables":{"probe":[{"id":"probe:maximum", "space_id":"scope", "text":text}]}
    }))
    .unwrap()
}

#[expect(
    clippy::panic,
    reason = "Unexpected native values are explicit failures of this regression oracle."
)]
fn value_string(report: &SnapshotInspection, key: &str) -> String {
    let NativeInspectionValue::Map(row) = report.rows[0].record.as_ref().unwrap() else {
        panic!("physical object required");
    };
    let NativeInspectionValue::String(text) = &row[key] else {
        panic!("physical string required");
    };
    text.clone()
}

#[test]
#[expect(
    clippy::panic,
    reason = "Unexpected native values are explicit failures of this regression oracle."
)]
fn maximum_source_worst_control_encoding_and_native_bytes_fit_finite_envelope() {
    use elephant_types::publication::MAX_SOURCE_TEXT_BYTES;
    assert_eq!(SnapshotInspectionLimits::default().string_bytes, 14_065_536);
    // Same two serialization layers as source_revision.payload: an original
    // JSON control string followed by the physical backend's export quoting.
    let raw = "\u{1}".repeat(MAX_SOURCE_TEXT_BYTES);
    let control = serde_json::to_string(&serde_json::json!({
        "revision":{"source":{"metadata":{"_manual_doc":{"text":raw}}}}
    }))
    .unwrap();
    assert!(control.len() >= MAX_SOURCE_TEXT_BYTES * 6);
    for backend in [
        StorageBackendKind::ElephantDb,
        StorageBackendKind::SurrealDb,
    ] {
        let payload = if backend == StorageBackendKind::ElephantDb {
            embedded_string_row(&control)
        } else {
            // Use the actual pinned native literal formatter; no handwritten
            // substitute for QuoteStr's escape or base64 representation.
            format!(
                "INSERT [{{id:probe:maximum,space_id:'scope',text:{}}}];",
                Value::from(control.clone())
            )
            .into_bytes()
        };
        assert!(payload.len() >= MAX_SOURCE_TEXT_BYTES * 7);
        let directory = bundle(&payload, backend);
        let report = inspect_snapshot(directory.path(), &full()).unwrap();
        assert_eq!(value_string(&report, "text"), control);
    }
    let bytes = vec![0xa5; MAX_SOURCE_TEXT_BYTES];
    let payload = format!(
        "INSERT [{{id:probe:maximum,space_id:'scope',value:{}}}];",
        Value::Bytes(bytes.clone().into())
    );
    let directory = bundle(payload.as_bytes(), StorageBackendKind::SurrealDb);
    let report = inspect_snapshot(directory.path(), &full()).unwrap();
    let NativeInspectionValue::Map(row) = report.rows[0].record.as_ref().unwrap() else {
        panic!("physical object required");
    };
    assert_eq!(
        row["value"],
        NativeInspectionValue::Bytes(hex::encode(bytes))
    );
}

#[test]
fn encoded_string_and_native_bytes_accept_exact_limit_and_refuse_one_over() {
    let cap = SnapshotInspectionLimits::default().string_bytes;
    for backend in [
        StorageBackendKind::ElephantDb,
        StorageBackendKind::SurrealDb,
    ] {
        for size in [cap, cap + 1] {
            let text = "x".repeat(size);
            let payload = if backend == StorageBackendKind::ElephantDb {
                embedded_string_row(&text)
            } else {
                format!(
                    "INSERT [{{id:probe:maximum,text:{}}}];",
                    Value::from(text.clone())
                )
                .into_bytes()
            };
            let directory = bundle(&payload, backend);
            if size == cap {
                assert_eq!(
                    value_string(
                        &inspect_snapshot(directory.path(), &full()).unwrap(),
                        "text"
                    ),
                    text
                );
            } else {
                refused(directory.path(), &full());
            }
        }
    }
    // The byte export's base64 token participates in the same encoded cap.
    // 768 bytes encode to exactly 1024 bytes; 769 encode to 1026 (no padding).
    let limits = SnapshotInspectionLimits {
        string_bytes: 1024,
        ..full()
    };
    for size in [768, 769] {
        let payload = format!(
            "INSERT [{{id:probe:maximum,value:{}}}];",
            Value::Bytes(vec![0xa5; size].into())
        );
        let directory = bundle(payload.as_bytes(), StorageBackendKind::SurrealDb);
        if size == 768 {
            assert_eq!(
                inspect_snapshot(directory.path(), &limits)
                    .unwrap()
                    .row_count,
                1
            );
        } else {
            refused(directory.path(), &limits);
        }
    }
    // A literal escaped token is charged for spelling, not only decoded bytes.
    for count in [512, 513] {
        let text = "\\".repeat(count);
        for backend in [
            StorageBackendKind::ElephantDb,
            StorageBackendKind::SurrealDb,
        ] {
            let payload = if backend == StorageBackendKind::ElephantDb {
                embedded_string_row(&text)
            } else {
                format!(
                    "INSERT [{{id:probe:maximum,text:{}}}];",
                    Value::from(text.clone())
                )
                .into_bytes()
            };
            let directory = bundle(&payload, backend);
            if count == 512 {
                assert_eq!(
                    value_string(
                        &inspect_snapshot(directory.path(), &limits).unwrap(),
                        "text"
                    ),
                    text
                );
            } else {
                refused(directory.path(), &limits);
            }
        }
    }
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
#[expect(
    clippy::too_many_lines,
    reason = "Keep the complete ordered conformance scenario and its failure assertions together."
)]
async fn actual_maximum_source_acceptance_export_is_completely_inspected() {
    use crate::source_publication::{AcceptSourceRevision, SourcePublicationRepository};
    use crate::{Repository, SpaceConfigRepository};
    use elephant_types::publication::{SourcePrecondition, MAX_SOURCE_TEXT_BYTES};
    use elephant_types::records::{BaseFields, Source, SourceType, SpaceConfig};
    use elephant_types::{RecordId, TableName};
    for backend in [
        StorageBackendKind::ElephantDb,
        StorageBackendKind::SurrealDb,
    ] {
        let root = tempfile::tempdir().unwrap();
        let storage = crate::StorageClient::new_with_durability(
            crate::StorageConfig {
                backend,
                ..Default::default()
            },
            None,
            Some(root.path().join("journal")),
        )
        .await
        .unwrap();
        if backend == StorageBackendKind::SurrealDb {
            storage
                .query_checked(include_str!(
                    "../../migrations/sql/031_source_publication.surql"
                ))
                .await
                .unwrap();
        }
        let config: SpaceConfig = serde_json::from_value(serde_json::json!({
            "id":RecordId::new_v4(TableName::SpaceConfig),"space_id":"maximum-source",
            "created_at":Utc::now(),"updated_at":Utc::now(),"created_by":"test",
            "security":{"policy_id":"default","level":0,"labels":[],"handling":[]},
            "provenance":{},"subjects":[],"subjects_state":"complete",
            "embedding_provider":"fake","embedding_model":"test","embedding_dims":8
        }))
        .unwrap();
        SpaceConfigRepository::new(storage.clone())
            .create(config)
            .await
            .unwrap();
        // Admission persists both the original Source and the revision's
        // structured state/JSON payload. Worst-case control characters expose
        // aggregate escaping that a large ASCII-only source does not exercise.
        let text = "\u{0001}".repeat(MAX_SOURCE_TEXT_BYTES);
        let mut source = Source::new(
            BaseFields::new(
                RecordId::new_v4(TableName::Source),
                "maximum-source".into(),
                "test".into(),
            ),
            SourceType::Manual,
            crate::snapshot::digest(text.as_bytes()),
        );
        source.metadata = serde_json::json!({"_manual_doc":{"text":text}});
        let request = AcceptSourceRevision {
            source,
            revision_id: "max-r1".into(),
            expected: SourcePrecondition::Absent,
            idempotency_key: "max-accept".into(),
            request_fingerprint: "max-fixture".into(),
            reactivate: false,
            recreate_after_erasure: false,
            authorized: None,
            authorized_document: None,
        };
        let accepted = SourcePublicationRepository::new(storage.clone())
            .accept(request.clone())
            .await
            .unwrap();
        let mut oversized = request;
        oversized.source.base.id = RecordId::new_v4(TableName::Source);
        oversized.source.metadata["_manual_doc"]["text"] =
            serde_json::Value::String("x".repeat(MAX_SOURCE_TEXT_BYTES + 1));
        oversized.idempotency_key = "max-plus-one".into();
        assert!(SourcePublicationRepository::new(storage.clone())
            .accept(oversized)
            .await
            .is_err());
        let output = root.path().join("bundle");
        crate::snapshot::export_named(
            &storage,
            &output,
            "actual-maximum",
            &crate::snapshot::SnapshotRetention::default(),
        )
        .await
        .unwrap();
        let uncompressed = inspect_snapshot(&output, &full()).unwrap_err();
        assert!(uncompressed.to_string().contains("report_limit"));
        let report = inspect_snapshot(
            &output,
            &SnapshotInspectionLimits {
                compact_records: true,
                ..full()
            },
        )
        .unwrap();
        let report_bytes = serde_json::to_vec(&report).unwrap().len();
        eprintln!(
            "actual maximum admitted compact inventory: backend={backend:?}, payload_bytes={}, report_bytes={report_bytes}, rows={}",
            report.manifest.bytes, report.row_count
        );
        assert!(report_bytes <= SnapshotInspectionLimits::default().report_bytes);
        assert_eq!(report.format, "recall-snapshot-inspection-v2");
        assert!(report.rows.iter().all(|row| row.record.is_some()));
        assert_eq!(report.table_counts["source"], 1);
        assert_eq!(report.table_counts["source_revision"], 1);
        let original = SourcePublicationRepository::new(storage)
            .revision("maximum-source", &accepted.source_id, "max-r1")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            original.source.metadata["_manual_doc"]["text"]
                .as_str()
                .unwrap()
                .len(),
            MAX_SOURCE_TEXT_BYTES
        );
    }
}
