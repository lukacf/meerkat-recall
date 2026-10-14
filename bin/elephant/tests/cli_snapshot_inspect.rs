//! The installed CLI consumes actual named exports without a service or signer.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::path::Path;

use elephant_storage::snapshot::{export_named, SnapshotRetention};
use elephant_storage::{
    DocRepository, Repository, StorageBackendKind, StorageClient, StorageConfig,
};
use elephant_types::records::base::BaseFields;
use elephant_types::records::Doc;
use elephant_types::{RecordId, TableName};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tokio::process::Command;

fn command(directory: &Path, bundle: &Path) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_meerkat-recall"));
    command
        .env_clear()
        .env("PATH", "/usr/bin:/bin")
        .env("HOME", directory)
        .env(
            "ELEPHANT_API_BEARER_TOKEN_FILE",
            directory.join("missing-token"),
        )
        .env("JWT_SECRET_FILE", directory.join("missing-signing-key"))
        .env(
            "ELEPHANT_DURABILITY_PATH",
            directory.join("missing-journal"),
        )
        .current_dir(directory)
        .args([
            "--api-url",
            "http://127.0.0.1:1",
            "snapshot",
            "inspect",
            "--input",
        ])
        .arg(bundle);
    command
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "Keep the offline snapshot inspection, privacy, and read-only checks in one scenario."
)]
async fn actual_named_export_inspection_is_offline_private_by_default_and_read_only() {
    for backend in [
        StorageBackendKind::ElephantDb,
        StorageBackendKind::SurrealDb,
    ] {
        let directory = tempfile::tempdir().unwrap();
        let client = StorageClient::new_with_durability(
            StorageConfig {
                backend,
                ..StorageConfig::default()
            },
            None,
            Some(directory.path().join("original-journal")),
        )
        .await
        .unwrap();
        let canary = "original-private-snapshot-inspector-payload-650211";
        let long_value = "original-private-compact-value-885931".repeat(128);
        let long_sha256 = hex::encode(Sha256::digest(long_value.as_bytes()));
        let doc = DocRepository::new(client.clone())
            .create(Doc {
                base: BaseFields::new(
                    RecordId::new_v4(TableName::Doc),
                    "snapshot-inspection-space".into(),
                    "cli-inspection-test".into(),
                ),
                title: "Inspection fixture".into(),
                text: canary.into(),
                source: None,
                sidecar_text: None,
                media_attachments: vec![],
                rendition_key: "inspection".into(),
                metadata: json!({"original":true, "long_value":long_value}),
            })
            .await
            .unwrap();
        let bundle = directory.path().join("named-backup");
        let manifest = export_named(
            &client,
            &bundle,
            "inspection",
            &SnapshotRetention::default(),
        )
        .await
        .unwrap();
        drop(client);
        let original_manifest = std::fs::read(bundle.join("manifest.json")).unwrap();
        let original_payload = std::fs::read(bundle.join("data.snapshot")).unwrap();
        let before_entries = std::fs::read_dir(directory.path()).unwrap().count();

        for (include_records, compact_records) in [(false, false), (true, false), (true, true)] {
            let mut process = command(directory.path(), &bundle);
            process.arg("--json");
            if include_records {
                process.arg("--include-records");
            }
            if compact_records {
                process.arg("--compact-records");
            }
            let output = process.output().await.unwrap();
            assert!(
                output.status.success(),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
            let report: Value = serde_json::from_slice(&output.stdout).unwrap();
            assert_eq!(
                report["format"],
                if compact_records {
                    "recall-snapshot-inspection-v2"
                } else {
                    "recall-snapshot-inspection-v1"
                }
            );
            assert_eq!(report.get("blobs").is_some(), compact_records);
            if compact_records {
                assert_eq!(
                    report["blobs"][long_sha256.as_str()]["bytes"],
                    long_value.len()
                );
                assert!(!String::from_utf8_lossy(&output.stdout).contains(&long_value));
            } else {
                assert_eq!(
                    String::from_utf8_lossy(&output.stdout).contains(&long_value),
                    include_records
                );
            }
            assert_eq!(report["manifest"], serde_json::to_value(&manifest).unwrap());
            assert_eq!(report["table_counts"]["doc"], 1);
            assert!(report["row_count"].as_u64().unwrap() >= 1);
            let rows = report["rows"].as_array().unwrap();
            let docs: Vec<_> = rows
                .iter()
                .filter(|row| row["identity"]["table"] == "doc")
                .collect();
            assert_eq!(docs.len(), 1);
            assert_eq!(docs[0]["space_id"], "snapshot-inspection-space");
            assert!(serde_json::to_string(&docs[0]["identity"])
                .unwrap()
                .contains(&doc.base.id.id().to_string()));
            assert_eq!(
                String::from_utf8_lossy(&output.stdout).contains(canary),
                include_records
            );
            assert!(!String::from_utf8_lossy(&output.stderr).contains(canary));
            if include_records {
                assert!(docs[0].get("record").is_some());
            } else {
                assert!(rows.iter().all(|row| row.get("record").is_none()));
            }
            assert_eq!(
                std::fs::read(bundle.join("manifest.json")).unwrap(),
                original_manifest
            );
            assert_eq!(
                std::fs::read(bundle.join("data.snapshot")).unwrap(),
                original_payload
            );
            assert_eq!(
                std::fs::read_dir(directory.path()).unwrap().count(),
                before_entries
            );
            assert!(!directory.path().join(".elephant").exists());
            assert!(!directory.path().join("missing-journal").exists());
        }

        let plain = command(directory.path(), &bundle).output().await.unwrap();
        assert!(plain.status.success());
        assert!(!String::from_utf8_lossy(&plain.stdout).contains(canary));
        let mut damaged = original_payload.clone();
        damaged.push(b'!');
        std::fs::write(bundle.join("data.snapshot"), &damaged).unwrap();
        let refused = command(directory.path(), &bundle)
            .arg("--json")
            .output()
            .await
            .unwrap();
        assert_eq!(refused.status.code(), Some(1));
        assert!(refused.stdout.is_empty());
        assert!(!String::from_utf8_lossy(&refused.stderr).contains(canary));
        assert_eq!(
            std::fs::read(bundle.join("data.snapshot")).unwrap(),
            damaged
        );
    }
}

#[tokio::test]
async fn raw_inventory_requires_explicit_json_and_missing_bundle_never_calls_api() {
    let directory = tempfile::tempdir().unwrap();
    let absent = directory.path().join("absent");
    let usage = command(directory.path(), &absent)
        .arg("--include-records")
        .output()
        .await
        .unwrap();
    assert_eq!(usage.status.code(), Some(2));
    assert!(usage.stdout.is_empty());
    for arguments in [
        vec!["--compact-records"],
        vec!["--compact-records", "--json"],
        vec!["--compact-records", "--include-records"],
    ] {
        let usage = command(directory.path(), &absent)
            .args(arguments)
            .output()
            .await
            .unwrap();
        assert_eq!(usage.status.code(), Some(2));
        assert!(usage.stdout.is_empty());
    }
    let failure = command(directory.path(), &absent)
        .arg("--json")
        .output()
        .await
        .unwrap();
    assert_eq!(failure.status.code(), Some(1));
    assert!(failure.stdout.is_empty());
    let stderr = String::from_utf8_lossy(&failure.stderr);
    assert!(!stderr.contains("management request"));
    assert!(!stderr.contains("missing-token"));
    assert!(!stderr.contains("missing-signing-key"));
    assert!(!directory.path().join(".elephant").exists());
    assert!(!directory.path().join("missing-journal").exists());
}
