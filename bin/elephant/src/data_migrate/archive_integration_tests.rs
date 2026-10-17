//! Actual producer plan through native knowledge plus journal-managed original rows.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
use super::*;
use elephant_storage::historical_archive_lifecycle::{
    verify_receipt_current, OperationalArchiveReceipt,
};
use elephant_storage::historical_operational_archive::{VerifiedArchive, OPERATIONAL_TABLES};
use elephant_types::records::{BaseFields, SystemEvent};
use elephant_types::{RecordId, TableName};

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Retain original client and event/checkpoint readers through full split inventory, sequence-floor and final journal identity assertions."
)]
async fn actual_python_archive_initial_cut_keeps_original_bytes_and_complete_split_inventory() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    let plan = integration_tests::planner_fixture_mode(root, "managed_archive_v1");
    let config = integration_tests::write_config(root);
    let scratch = root.join("checks.sqlite");
    let report = root.join("report.json");
    let receipts = root.join("adoption.jsonl");
    let spool = SourceSpool::open(&plan, &scratch).unwrap();
    let expected_inventory = spool.archive_inventory.clone().unwrap();
    let expected_work = spool.page("work_item", None).unwrap();
    let expected_events = spool.page("system_event", None).unwrap();
    let mut expected_hash = Sha256::new();
    for (table, count) in &spool.tables {
        expected_hash
            .update(serde_json::to_vec(&serde_json::json!(["table", table, count])).unwrap());
        expected_hash.update(b"\n");
    }
    for receipt in &spool.retained_controls {
        expected_hash
            .update(serde_json::to_vec(&serde_json::json!(["retained_control", receipt])).unwrap());
        expected_hash.update(b"\n");
    }
    for table in super::super::SUPPORTED_TABLES {
        if *table == "id_map" {
            continue;
        }
        let mut cursor = None;
        loop {
            let rows = spool.page(table, cursor.as_deref()).unwrap();
            if rows.is_empty() {
                break;
            }
            for row in &rows {
                super::super::archive_copy::hash_source_row(&mut expected_hash, table, row)
                    .unwrap();
            }
            cursor = rows.last().map(|row| row.id.clone());
        }
    }
    let expected_hash = hex::encode(expected_hash.finalize());
    let adoption = super::super::historical_adopt::prepare(
        &spool,
        &plan,
        &config,
        &root.join("adoption.sqlite"),
    )
    .unwrap();
    let digest = spool.source_sha256.clone();
    let saved_report = report.clone();
    let saved_scratch = scratch.clone();
    let client = StorageClient::new_memory().await.unwrap();
    client
        .with_transfer_barrier(&digest, move |exclusive| async move {
            copy(&spool, &exclusive, &scratch, &report, adoption, &receipts)
                .await
                .map_err(|e| elephant_storage::StorageError::DataIntegrityError(format!("{e:#}")))
        })
        .await
        .unwrap();
    let output: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&saved_report).unwrap()).unwrap();
    assert_eq!(output["operational_disposition"], "managed_archive_v1");
    assert_eq!(
        output["catchup_baseline"]["operational_disposition"],
        "managed_archive_v1"
    );
    assert_eq!(
        output["operational_archive"],
        output["catchup_baseline"]["operational_archive"]
    );
    assert_eq!(output["source_lexical_rows_sha256"], expected_hash);
    assert_eq!(
        output["archived_tables"],
        serde_json::to_value(&expected_inventory.tables).unwrap()
    );
    assert_eq!(
        output["archive_ordered_inventory_sha256"],
        expected_inventory.ordered_inventory_sha256
    );
    assert_eq!(
        output["archive_lexical_bytes"],
        expected_inventory.lexical_bytes
    );
    assert_eq!(
        output["source_accounting"]["rows"].as_u64(),
        Some(
            output["source_tables"]
                .as_object()
                .unwrap()
                .values()
                .map(|v| v.as_u64().unwrap())
                .sum()
        )
    );
    assert_eq!(
        output["source_accounting"]["tables"]["work_edge"]["disposition"],
        "declared_empty"
    );
    assert!(output["source_accounting"]["tables"]
        .get("work_deadletter")
        .is_none());
    assert_eq!(
        output["historical_work_holds"]["mode"],
        "managed_archive_inert"
    );
    assert_eq!(output["historical_work_holds"]["native_holds"], 0);
    assert_eq!(output["historical_work_holds"]["rows"]["work_item"], 1);
    assert_eq!(output["copied_tables"]["doc"], 3);
    assert_eq!(output["copied_tables"]["chunk_embedding"], 1);
    assert_eq!(output["entity_name_embedding"], 1);
    for table in OPERATIONAL_TABLES {
        assert_eq!(output["copied_tables"][table], 0);
        assert!(fingerprints(&client, table, None).await.unwrap().is_empty());
    }
    assert!(fingerprints(&client, "work_producer_origin", None)
        .await
        .unwrap()
        .is_empty());
    let receipt: OperationalArchiveReceipt =
        serde_json::from_value(output["operational_archive"].clone()).unwrap();
    verify_receipt_current(&client, &receipt).await.unwrap();
    // White-box fixture only: production administrative access obtains its path
    // from journal custody, never from a report or caller-supplied pathname.
    let archived_path = client
        .erasure_journal()
        .unwrap()
        .path()
        .join("operational-archives")
        .join(&receipt.registration.generation_id)
        .join("archive.sqlite");
    let original = VerifiedArchive::open(&archived_path, &receipt.binding, &receipt.seal).unwrap();
    for (table, rows) in [
        ("work_item", expected_work),
        ("system_event", expected_events),
    ] {
        for row in rows {
            let actual = original.row(table, &row.id).unwrap().unwrap();
            assert_eq!(actual.lexical, row.lexical.as_bytes());
            assert_eq!(
                actual.lexical_sha256,
                hex::encode(Sha256::digest(row.lexical.as_bytes()))
            );
        }
    }
    assert_eq!(
        output["catchup_baseline"]["expected_ledger_sha256"],
        source_spool::file_sha(&saved_scratch).unwrap()
    );
    let mut ledger = Connection::open(&saved_scratch).unwrap();
    let tables =
        serde_json::from_value(output["catchup_baseline"]["expected_tables"].clone()).unwrap();
    assert_eq!(
        reconcile(&client, &mut ledger, &tables).await.unwrap(),
        output["destination_native_rows_sha256"]
    );
    // Exercise the actual administrative entry point under storage-owned read
    // admission, including a forged report that passes lexical input validation.
    let admin_target = crate::migration_archive::ArchiveTarget {
        target: crate::data_migrate::TransferTarget {
            url: None,
            namespace: None,
            database: None,
        },
        report: saved_report.clone(),
        report_sha256: source_spool::file_sha(&saved_report).unwrap(),
    };
    let inspection = crate::migration_archive::run_inner(
        crate::migration_archive::MigrationArchiveCommand::Inspect(admin_target.clone()),
        Some(client.clone()),
    )
    .await
    .unwrap();
    assert_eq!(
        inspection["resolved_archive"],
        output["operational_archive"]
    );
    assert_eq!(inspection["surviving_tables"], output["archived_tables"]);
    for table in ["work_item", "system_event", "work_edge"] {
        let directory = root.join(format!("external-{table}"));
        let manifest = crate::migration_archive::run_inner(
            crate::migration_archive::MigrationArchiveCommand::Export(
                crate::migration_archive::ArchiveExportArgs {
                    archive: admin_target.clone(),
                    table: table.into(),
                    output: directory.clone(),
                },
            ),
            Some(client.clone()),
        )
        .await
        .unwrap();
        let bytes = std::fs::read(directory.join("rows.bin")).unwrap();
        let index = std::fs::read_to_string(directory.join("rows.jsonl")).unwrap();
        let rows = original.page(table, None, 256).unwrap();
        assert_eq!(manifest["rows"].as_u64(), Some(rows.len() as u64));
        assert_eq!(index.lines().count(), rows.len());
        for (line, row) in index.lines().zip(rows) {
            let entry: serde_json::Value = serde_json::from_str(line).unwrap();
            let offset = usize::try_from(entry["offset"].as_u64().unwrap()).unwrap();
            let size = usize::try_from(entry["bytes"].as_u64().unwrap()).unwrap();
            assert_eq!(&bytes[offset..offset + size], row.lexical);
            assert_eq!(entry["lexical_sha256"], row.lexical_sha256);
            assert_eq!(entry["id"], row.id);
        }
    }
    let mut forged = output.clone();
    forged["operational_archive"]["native_receipt_sha256"] = "0".repeat(64).into();
    forged["catchup_baseline"]["operational_archive"] = forged["operational_archive"].clone();
    let forged_path = root.join("forged-report.json");
    std::fs::write(&forged_path, serde_json::to_vec(&forged).unwrap()).unwrap();
    let forged_target = crate::migration_archive::ArchiveTarget {
        report_sha256: source_spool::file_sha(&forged_path).unwrap(),
        report: forged_path,
        ..admin_target.clone()
    };
    let denied_output = root.join("denied-external");
    assert!(crate::migration_archive::run_inner(
        crate::migration_archive::MigrationArchiveCommand::Export(
            crate::migration_archive::ArchiveExportArgs {
                archive: forged_target,
                table: "work_item".into(),
                output: denied_output.clone(),
            }
        ),
        Some(client.clone()),
    )
    .await
    .is_err());
    assert!(!denied_output.exists());
    let undeclared_output = root.join("undeclared-external");
    assert!(crate::migration_archive::run_inner(
        crate::migration_archive::MigrationArchiveCommand::Export(
            crate::migration_archive::ArchiveExportArgs {
                archive: admin_target,
                table: "work_deadletter".into(),
                output: undeclared_output.clone(),
            }
        ),
        Some(client.clone()),
    )
    .await
    .is_err());
    assert!(!undeclared_output.exists());
    let outbox = elephant_storage::outbox_checkpoint_repository::OutboxCheckpointRepository::new(
        client.clone(),
    );
    let events = elephant_storage::SystemEventRepository::new(client.clone());
    for (space, tail) in [("household", 17), ("foreign", 29)] {
        for consumer in elephant_storage::outbox_checkpoint_repository::HISTORICAL_CONSUMERS {
            assert_eq!(outbox.load(space, consumer).await.unwrap(), tail);
            assert!(events
                .get_after_seq(space, tail, 100)
                .await
                .unwrap()
                .is_empty());
        }
        let event = SystemEvent {
            base: BaseFields::new(
                RecordId::new_v4(TableName::SystemEvent),
                space.into(),
                "new-live".into(),
            ),
            seq: 0,
            event_type: "records.changed".into(),
            payload: serde_json::json!({"new":true}),
        };
        assert_eq!(
            elephant_storage::event_sequence::append_system_event(&client, event)
                .await
                .unwrap()
                .seq,
            tail + 1
        );
    }
    client.verify_journal_identity().await.unwrap();
}

#[test]
fn archive_plan_rejects_missing_requirement_and_same_count_lexical_replacement_before_target() {
    for mutation in ["requirement", "lexical", "inventory", "presence"] {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path();
        let path = integration_tests::planner_fixture_mode(root, "managed_archive_v1");
        let mut plan: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        let index = path.parent().unwrap().join("inventory.sqlite");
        let db = Connection::open(&index).unwrap();
        match mutation {
            "requirement" => {
                plan["runtime_requirements"] = serde_json::json!([]);
                db.execute(
                    "UPDATE metadata SET v='[]' WHERE k='runtime_requirements'",
                    [],
                )
                .unwrap();
            }
            "lexical" => {
                let (id, raw): (String, Vec<u8>) = db
                    .query_row(
                        "SELECT k,lexical FROM raw_rows WHERE t='work_item' LIMIT 1",
                        [],
                        |row| Ok((row.get(0)?, row.get(1)?)),
                    )
                    .unwrap();
                let mut raw = String::from_utf8(raw).unwrap();
                raw.push(' ');
                db.execute(
                    "UPDATE raw_rows SET lexical=?,lexical_sha256=? WHERE t='work_item' AND k=?",
                    params![
                        raw.as_bytes(),
                        hex::encode(Sha256::digest(raw.as_bytes())),
                        id
                    ],
                )
                .unwrap();
            }
            "inventory" => {
                plan["operational_archive_inventory"]["ordered_inventory_sha256"] =
                    "0".repeat(64).into();
                plan["runtime_requirements"][0]["source_inventory"] =
                    plan["operational_archive_inventory"].clone();
                for key in ["operational_archive_inventory", "runtime_requirements"] {
                    db.execute(
                        "UPDATE metadata SET v=? WHERE k=?",
                        params![plan[key].to_string(), key],
                    )
                    .unwrap();
                }
            }
            "presence" => {
                plan.as_object_mut()
                    .unwrap()
                    .remove("operational_archive_inventory");
            }
            _ => unreachable!(),
        }
        db.close().unwrap();
        plan["inventory_sha256"] = source_spool::file_sha(&index).unwrap().into();
        std::fs::write(&path, serde_json::to_vec(&plan).unwrap()).unwrap();
        assert!(
            SourceSpool::open(&path, &root.join("refused.sqlite")).is_err(),
            "{mutation}"
        );
    }
}

#[cfg(feature = "rocksdb")]
#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Preserve each original client through its explicit consuming close and immediate reopen assertions, including the borrowed-client liveness check."
)]
async fn archive_owned_admin_success_and_error_close_before_same_runtime_reopen() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    let plan = integration_tests::planner_fixture_mode(root, "managed_archive_v1");
    let config = integration_tests::write_config(root);
    let scratch = root.join("checks.sqlite");
    let report = root.join("report.json");
    let receipts = root.join("adoption.jsonl");
    let spool = SourceSpool::open(&plan, &scratch).unwrap();
    let adoption = super::super::historical_adopt::prepare(
        &spool,
        &plan,
        &config,
        &root.join("adoption.sqlite"),
    )
    .unwrap();
    let digest = spool.source_sha256.clone();
    let target = super::super::TransferTarget {
        url: Some(format!("rocksdb://{}", root.join("native").display())),
        namespace: Some("archive-close".into()),
        database: Some("archive-close".into()),
    };
    let saved_report = report.clone();
    let client = target.clone().connect().await.unwrap();
    client
        .with_transfer_barrier(&digest, move |exclusive| async move {
            copy(&spool, &exclusive, &scratch, &report, adoption, &receipts)
                .await
                .map_err(|error| {
                    elephant_storage::StorageError::DataIntegrityError(format!("{error:#}"))
                })
        })
        .await
        .unwrap();
    let original_bytes = std::fs::read(&saved_report).unwrap();
    let args = crate::migration_archive::ArchiveTarget {
        target: target.clone(),
        report_sha256: source_spool::file_sha(&saved_report).unwrap(),
        report: saved_report.clone(),
    };
    let borrowed = crate::migration_archive::run_inner(
        crate::migration_archive::MigrationArchiveCommand::Inspect(args.clone()),
        Some(client.clone()),
    )
    .await
    .unwrap();
    // A caller-supplied client stays live after administrative read admission.
    client.verify_journal_identity().await.unwrap();
    super::super::finish_owned_connection(client, Ok(()))
        .await
        .unwrap();
    let owned = crate::migration_archive::run_inner(
        crate::migration_archive::MigrationArchiveCommand::Inspect(args.clone()),
        None,
    )
    .await
    .unwrap();
    assert_eq!(owned, borrowed);
    // The same runtime can acquire both the writer lock and native engine now.
    let reopened = target.clone().connect().await.unwrap();
    reopened.verify_journal_identity().await.unwrap();
    super::super::finish_owned_connection(reopened, Ok(()))
        .await
        .unwrap();
    let output = root.join("denied-export");
    let error = crate::migration_archive::run_inner(
        crate::migration_archive::MigrationArchiveCommand::Export(
            crate::migration_archive::ArchiveExportArgs {
                archive: args,
                table: "work_deadletter".into(),
                output: output.clone(),
            },
        ),
        None,
    )
    .await
    .unwrap_err();
    assert!(format!("{error:#}").contains("table was not declared in the managed archive"));
    assert!(!output.exists());
    assert_eq!(std::fs::read(saved_report).unwrap(), original_bytes);
    let reopened = target.connect().await.unwrap();
    reopened.verify_journal_identity().await.unwrap();
    super::super::finish_owned_connection(reopened, Ok(()))
        .await
        .unwrap();
}

#[cfg(feature = "rocksdb")]
#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Preserve distinct original and reopened clients through exact pending-marker checks, explicit consuming closes and immediate same-runtime reopen."
)]
async fn owned_discard_success_and_refusal_close_before_same_runtime_reopen() {
    let directory = tempfile::tempdir().unwrap();
    let target = super::super::TransferTarget {
        url: Some(format!(
            "rocksdb://{}",
            directory.path().join("native").display()
        )),
        namespace: Some("discard-close".into()),
        database: Some("discard-close".into()),
    };
    let digest = "a".repeat(64);
    let client = target.clone().connect().await.unwrap();
    let failed: elephant_storage::StorageResult<()> = client
        .with_transfer_barrier(&digest, |exclusive| async move {
            exclusive
                .query_checked("CREATE transfer_probe:partial SET value='partial'")
                .await?;
            Err(elephant_storage::StorageError::DataIntegrityError(
                "injected partial transfer".into(),
            ))
        })
        .await;
    assert!(failed.is_err());
    let marker_path = client
        .erasure_journal()
        .unwrap()
        .path()
        .join("transfer-pending.json");
    let marker = std::fs::read(&marker_path).unwrap();
    let decoded: serde_json::Value = serde_json::from_slice(&marker).unwrap();
    assert_eq!(decoded["source_sha256"], digest);
    super::super::finish_owned_connection(client, Ok(()))
        .await
        .unwrap();
    let error = super::super::discard(super::super::DataMigrateDiscardArgs {
        target: target.clone(),
        source_sha256: "b".repeat(64),
        discard_target: true,
    })
    .await
    .unwrap_err();
    assert!(format!("{error:#}")
        .contains("incomplete transfer identity does not match discard request"));
    let reopened = target.clone().connect().await.unwrap();
    assert!(!reopened.recovery_status().ready);
    assert_eq!(std::fs::read(&marker_path).unwrap(), marker);
    super::super::finish_owned_connection(reopened, Ok(()))
        .await
        .unwrap();
    super::super::discard(super::super::DataMigrateDiscardArgs {
        target: target.clone(),
        source_sha256: digest,
        discard_target: true,
    })
    .await
    .unwrap();
    let reopened = target.connect().await.unwrap();
    assert!(reopened.recovery_status().ready);
    assert!(!marker_path.exists());
    assert!(reopened.list_tables().await.unwrap().is_empty());
    super::super::finish_owned_connection(reopened, Ok(()))
        .await
        .unwrap();
}
