//! Actual Python initial transfer through current archive custody and lifecycle.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
use super::*;
use elephant_storage::historical_archive_lifecycle::{
    with_operational_archive_read, OperationalArchiveReceipt,
};
use elephant_storage::source_publication::SourceMutation;
use elephant_storage::{DocRepository, Repository, SourcePublicationRepository};
use elephant_types::publication::{OperationState, SourcePrecondition};
use elephant_types::RecordId;
use std::path::PathBuf;

const SECRET: &str = "ARCHIVE_NATIVE_ERASURE_CANARY_c6267054";
fn planner(root: &Path) -> std::path::PathBuf {
    integration_tests::planner_fixture_mode(root, "managed_archive_v1");
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let code = r"
import json,sys
from pathlib import Path
sys.path.insert(0,str(Path(sys.argv[1])/'scripts/migration'))
import preservation_plan as p
root=Path(sys.argv[2]);original=json.loads((root/'snapshot.json').read_bytes())
work=original['tables']['work_item'][0]
work['payload']={'nested':{'document':'doc:00000000-0000-4000-8000-000000000001'},'canary':sys.argv[3]}
foreign=json.loads(json.dumps(work));foreign['id']='work_item:00000000-0000-4000-8000-000000000077'
foreign['space_id']='foreign';foreign['idempotency_key']='foreign-history';foreign['payload']['canary']='independent foreign archive'
foreign['payload']['nested']['document']='doc:00000000-0000-4000-8000-000000000003'
original['tables']['work_item'].append(foreign)
cut=root/'lifecycle-snapshot.json';cut.write_bytes(p.canonical(original))
idmap=root/'idmap.jsonl'
p.inventory(cut,idmap,root/'lifecycle-plan',space='default',target_space='household',snapshot_sha256=p.file_sha(cut),idmap_sha256=p.file_sha(idmap),operational_disposition='managed_archive_v1')
";
    let result = std::process::Command::new("python3")
        .args(["-c", code])
        .arg(&workspace)
        .arg(root)
        .arg(SECRET)
        .env("PYTHONDONTWRITEBYTECODE", "1")
        .output()
        .unwrap();
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    root.join("lifecycle-plan/plan.json")
}
async fn initial(
    root: &Path,
    client: &StorageClient,
    fail_after_copy: bool,
) -> (PathBuf, String, OperationalArchiveReceipt) {
    let plan = planner(root);
    initial_plan(root, &plan, client, fail_after_copy).await
}
async fn initial_plan(
    root: &Path,
    plan: &Path,
    client: &StorageClient,
    fail_after_copy: bool,
) -> (PathBuf, String, OperationalArchiveReceipt) {
    let config = integration_tests::write_config(root);
    let scratch = root.join("checks.sqlite");
    let report = root.join("report.json");
    let receipts = root.join("adoption.jsonl");
    let spool = SourceSpool::open(plan, &scratch).unwrap();
    let digest = spool.source_sha256.clone();
    let adoption = super::super::historical_adopt::prepare(
        &spool,
        plan,
        &config,
        &root.join("adoption.sqlite"),
    )
    .unwrap();
    let saved_report = report.clone();
    let result = client
        .with_transfer_barrier(&digest, move |exclusive| async move {
            Box::pin(copy(
                &spool, &exclusive, &scratch, &report, adoption, &receipts,
            ))
            .await
            .map_err(|e| elephant_storage::StorageError::DataIntegrityError(format!("{e:#}")))?;
            if fail_after_copy {
                return Err(elephant_storage::StorageError::DataIntegrityError(
                    "injected after archive report before transfer completion".into(),
                ));
            }
            Ok(())
        })
        .await;
    if fail_after_copy {
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("injected after archive report"));
    } else {
        result.unwrap();
    }
    let output: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&saved_report).unwrap()).unwrap();
    let receipt = serde_json::from_value(output["operational_archive"].clone()).unwrap();
    (saved_report, digest, receipt)
}
async fn archived(
    client: &StorageClient,
    receipt: &OperationalArchiveReceipt,
) -> (
    OperationalArchiveReceipt,
    Vec<elephant_storage::historical_operational_archive::ArchiveRow>,
) {
    with_operational_archive_read(client, receipt, |reader| {
        Ok((
            reader.receipt().clone(),
            reader.page("work_item", None, 256)?,
        ))
    })
    .await
    .unwrap()
}
#[expect(
    clippy::significant_drop_tightening,
    reason = "Retain original publication and document readers through erasure, idempotent replay and final document absence readback."
)]
async fn erase(client: &StorageClient) {
    let docs = DocRepository::new(client.clone());
    let id: RecordId = "doc:00000000-0000-4000-8000-000000000001".parse().unwrap();
    let doc = docs.get("household", &id).await.unwrap().unwrap();
    let source = doc.source.as_ref().unwrap().id().clone();
    let publication = SourcePublicationRepository::new(client.clone());
    let head = publication
        .head("household", &source)
        .await
        .unwrap()
        .unwrap();
    let request = SourceMutation {
        space_id: "household".into(),
        source_id: source.clone(),
        expected: SourcePrecondition::Generation(head.generation),
        idempotency_key: "archive-initial-erase".into(),
        request_fingerprint: "archive-initial-erase-v1".into(),
        actor: "authorized-fixture".into(),
        authorized: publication
            .authorization_snapshot("household", &source)
            .await
            .unwrap()
            .unwrap(),
        authorized_document: None,
    };
    let receipt = Box::pin(publication.erase_live(request.clone()))
        .await
        .unwrap();
    assert_eq!(receipt.state, OperationState::Erased);
    assert_eq!(
        Box::pin(publication.erase_live(request)).await.unwrap(),
        receipt
    );
    assert!(docs.get("household", &id).await.unwrap().is_none());
}
fn assert_survivors(
    original: &[elephant_storage::historical_operational_archive::ArchiveRow],
    rows: &[elephant_storage::historical_operational_archive::ArchiveRow],
) {
    assert_eq!(original.len(), 2);
    assert_eq!(rows.len(), 1);
    let foreign = original
        .iter()
        .find(|r| {
            serde_json::from_slice::<serde_json::Value>(&r.lexical).unwrap()["space_id"]
                == "foreign"
        })
        .unwrap();
    assert_eq!(rows[0].id, foreign.id);
    assert_eq!(rows[0].lexical, foreign.lexical);
    assert_eq!(rows[0].lexical_sha256, foreign.lexical_sha256);
}
fn assert_managed_archive_bytes_erased(client: &StorageClient, old: &OperationalArchiveReceipt) {
    let root = client
        .erasure_journal()
        .unwrap()
        .path()
        .join("operational-archives");
    assert!(!root
        .join(&old.registration.generation_id)
        .join("archive.sqlite")
        .exists());
    for generation in std::fs::read_dir(root).unwrap() {
        let generation = generation.unwrap();
        if !generation.file_type().unwrap().is_dir() {
            continue;
        }
        for file in std::fs::read_dir(generation.path()).unwrap() {
            let file = file.unwrap();
            if !file.file_type().unwrap().is_file() {
                continue;
            }
            let bytes = std::fs::read(file.path()).unwrap();
            assert!(
                !bytes
                    .windows(SECRET.len())
                    .any(|window| window == SECRET.as_bytes()),
                "managed payload canary in {}",
                file.path().display()
            );
        }
    }
}
async fn check_admin(
    root: &Path,
    client: &StorageClient,
    report: &Path,
    expected: &OperationalArchiveReceipt,
) {
    let args = crate::migration_archive::ArchiveTarget {
        target: crate::data_migrate::TransferTarget {
            url: None,
            namespace: None,
            database: None,
        },
        report: report.into(),
        report_sha256: source_spool::file_sha(report).unwrap(),
    };
    let inspection = crate::migration_archive::run_inner(
        crate::migration_archive::MigrationArchiveCommand::Inspect(args.clone()),
        Some(client.clone()),
    )
    .await
    .unwrap();
    assert_eq!(
        inspection["resolved_archive"],
        serde_json::to_value(expected).unwrap()
    );
    assert_eq!(inspection["surviving_tables"]["work_item"], 1);
    let output = root.join("survivor-external");
    crate::migration_archive::run_inner(
        crate::migration_archive::MigrationArchiveCommand::Export(
            crate::migration_archive::ArchiveExportArgs {
                archive: args,
                table: "work_item".into(),
                output: output.clone(),
            },
        ),
        Some(client.clone()),
    )
    .await
    .unwrap();
    let bytes = std::fs::read(output.join("rows.bin")).unwrap();
    assert!(!bytes
        .windows(SECRET.len())
        .any(|window| window == SECRET.as_bytes()));
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep the original client through failed dependency restore, physical erasure, successful restore and final current archive administrative checks."
)]
async fn archive_initial_backup_erasure_resolves_original_report_and_blocks_missing_dependency() {
    Box::pin(async {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let client = StorageClient::new_memory().await.unwrap();
        let (report, _, original) = initial(root, &client, false).await;
        let (_, original_rows) = archived(&client, &original).await;
        let backup = root.join("before-erasure");
        elephant_storage::snapshot::export_named(
            &client,
            &backup,
            "original-archive-cut",
            &elephant_storage::snapshot::SnapshotRetention::default(),
        )
        .await
        .unwrap();
        let sidecar = backup.join("operational-archive-origin.json");
        let exact = std::fs::read(&sidecar).unwrap();
        let parked = backup.join("parked-origin.json");
        std::fs::rename(&sidecar, &parked).unwrap();
        assert!(elephant_storage::snapshot::restore_named(
            &client,
            &backup,
            client.erasure_journal().unwrap()
        )
        .await
        .is_err());
        assert!(client.recovery_status().ready);
        assert_eq!(archived(&client, &original).await.0, original);
        std::fs::rename(&parked, &sidecar).unwrap();
        assert_eq!(std::fs::read(&sidecar).unwrap(), exact);
        erase(&client).await;
        let (survivor, rows) = archived(&client, &original).await;
        assert_survivors(&original_rows, &rows);
        assert!(survivor.seal.redacted);
        assert_ne!(
            survivor.registration.generation_id,
            original.registration.generation_id
        );
        assert_managed_archive_bytes_erased(&client, &original);
        // A same-cut pre-erasure native backup resolves current independent
        // redaction authority and replays erasure before admitting reads.
        elephant_storage::snapshot::restore_named(
            &client,
            &backup,
            client.erasure_journal().unwrap(),
        )
        .await
        .unwrap();
        assert!(client.recovery_status().ready);
        let (after_restore, rows) = archived(&client, &original).await;
        assert_eq!(after_restore, survivor);
        assert_survivors(&original_rows, &rows);
        assert_managed_archive_bytes_erased(&client, &original);
        check_admin(root, &client, &report, &survivor).await;
    })
    .await;
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep the original client through wrong-marker refusal, exact discard, managed-byte removal, reimport and final ready assertion."
)]
async fn archive_initial_committed_failure_discard_requires_original_marker_and_removes_managed_bytes(
) {
    Box::pin(async {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let client = StorageClient::new_memory().await.unwrap();
        let (_, digest, receipt) = initial(root, &client, true).await;
        assert!(!client.recovery_status().ready);
        let payload = client
            .erasure_journal()
            .unwrap()
            .path()
            .join("operational-archives")
            .join(&receipt.registration.generation_id)
            .join("archive.sqlite");
        let before = std::fs::read(&payload).unwrap();
        assert!(before.windows(SECRET.len()).any(|w| w == SECRET.as_bytes()));
        assert!(client
            .discard_incomplete_transfer(&"0".repeat(64))
            .await
            .is_err());
        assert_eq!(std::fs::read(&payload).unwrap(), before);
        client.discard_incomplete_transfer(&digest).await.unwrap();
        assert!(!payload.exists());
        assert!(client.recovery_status().ready);
        assert!(client.list_tables().await.unwrap().is_empty());
        assert!(with_operational_archive_read(&client, &receipt, |_| Ok(()))
            .await
            .is_err());
        let retry = root.join("retry");
        std::fs::create_dir(&retry).unwrap();
        let (_, retried_digest, retried_receipt) = initial_plan(
            &retry,
            &root.join("lifecycle-plan/plan.json"),
            &client,
            false,
        )
        .await;
        assert_eq!(retried_digest, digest);
        assert_eq!(retried_receipt.binding, receipt.binding);
        assert_eq!(archived(&client, &retried_receipt).await.1.len(), 2);
        assert!(client.recovery_status().ready);
    })
    .await;
}

#[cfg(feature = "rocksdb")]
async fn persistent(root: &Path) -> StorageClient {
    StorageClient::new_with_durability(
        elephant_storage::StorageConfig {
            url: format!("rocksdb://{}", root.join("database").display()),
            ..elephant_storage::StorageConfig::default()
        },
        None,
        Some(root.join("independent-journal")),
    )
    .await
    .unwrap()
}
#[cfg(feature = "rocksdb")]
#[test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep each runtime-owned client through all erasure and survivor assertions; preserve original writer runtime teardown before the sole native reopen."
)]
fn archive_initial_real_erase_reopen_keeps_exact_survivor_and_original_report_authority() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    let writer = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let (report, original, survivor, original_rows, journal_id) =
        writer.block_on(Box::pin(async {
            let client = persistent(root).await;
            let (report, _, original) = initial(root, &client, false).await;
            let (_, original_rows) = archived(&client, &original).await;
            erase(&client).await;
            let (survivor, rows) = archived(&client, &original).await;
            assert_survivors(&original_rows, &rows);
            assert_managed_archive_bytes_erased(&client, &original);
            let journal_id = client.erasure_journal().unwrap().identity().to_owned();
            (report, original, survivor, original_rows, journal_id)
        }));
    // Drop the SDK router's runtime owner before the sole reopen, without a
    // sleep or failed-open retry hiding a retained physical engine lock.
    drop(writer);
    let reader = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    reader.block_on(Box::pin(async {
        let client = persistent(root).await;
        assert!(client.recovery_status().ready);
        assert_eq!(client.erasure_journal().unwrap().identity(), journal_id);
        let (resolved, rows) = archived(&client, &original).await;
        assert_eq!(resolved, survivor);
        assert_survivors(&original_rows, &rows);
        assert_managed_archive_bytes_erased(&client, &original);
        check_admin(root, &client, &report, &survivor).await;
        let doc: RecordId = "doc:00000000-0000-4000-8000-000000000001".parse().unwrap();
        assert!(DocRepository::new(client.clone())
            .get("household", &doc)
            .await
            .unwrap()
            .is_none());
        let foreign: RecordId = "doc:00000000-0000-4000-8000-000000000003".parse().unwrap();
        assert!(DocRepository::new(client)
            .get("foreign", &foreign)
            .await
            .unwrap()
            .is_some());
    }));
}
