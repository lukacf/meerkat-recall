//! Native-codec fixture controls plus active Mem backup/restore integration.
//! These do not substitute for an installed historical-engine erasure oracle.
#![allow(clippy::unwrap_used, clippy::expect_used)]
use super::*;
use crate::durability_io::private_directory;
use crate::{StorageBackendKind, StorageConfig};
use chrono::Utc;
use surrealdb::sql::{Object, Thing, Value};

fn journal(root: &Path) -> ErasureJournal {
    ErasureJournal::open(root.join("journal")).unwrap()
}

fn record(namespace: &str, database: &str, version: u64, value: &str) -> RecallHistoryRecord {
    let identity = Thing::from(("entity", "original"));
    let value = Value::Object(Object::from(BTreeMap::from([
        ("id".to_owned(), Value::Thing(identity.clone())),
        ("space_id".to_owned(), "named-Å/研发".into()),
        ("payload".to_owned(), value.into()),
    ])));
    RecallHistoryRecord {
        key: surrealdb::kvs::recall_history::record_key(
            namespace,
            database,
            &identity.tb,
            &identity.id,
        )
        .unwrap(),
        value: RecallHistoryRecord::encode_value(&value).unwrap(),
        version,
        timestamp: if version == 0 { 0 } else { u64::MAX - version },
        current: true,
        tombstone: false,
    }
}

fn snapshot(
    namespace: &str,
    database: &str,
    records: Vec<RecallHistoryRecord>,
) -> RecallHistorySnapshot {
    RecallHistorySnapshot::new(namespace.into(), database.into(), records).unwrap()
}

fn manifest(journal: &ErasureJournal) -> SnapshotManifest {
    SnapshotManifest {
        version: 1,
        format: "recall-snapshot-v1".into(),
        snapshot_id: uuid::Uuid::new_v4().to_string(),
        journal_id: journal.identity().into(),
        backend: StorageBackendKind::SurrealDb,
        name: "bounded-origin-test".into(),
        created_at: Utc::now(),
        expires_at: Utc::now() + chrono::Duration::hours(1),
        bytes: 4,
        sha256: crate::snapshot::digest(b"data"),
    }
}

#[expect(
    clippy::needless_pass_by_value,
    reason = "Keep the fixture-owned native value and authority construction interface unchanged."
)]
fn proof(
    root: &Path,
    journal: &ErasureJournal,
    snapshot: &RecallHistorySnapshot,
    causes: BTreeSet<String>,
) -> (std::path::PathBuf, SnapshotManifest, VerifiedBackupOrigins) {
    let output = root.join(uuid::Uuid::new_v4().to_string());
    private_directory(&output).unwrap();
    let manifest = manifest(journal);
    let causality = current_records(journal, snapshot)
        .unwrap()
        .into_keys()
        .map(|key| (key, causes.clone()))
        .collect();
    write_backup(journal, &output, &manifest, snapshot, causality).unwrap();
    let verified = verify_backup(
        journal,
        &output,
        &manifest,
        &snapshot.namespace,
        &snapshot.database,
    )
    .unwrap();
    (output, manifest, verified)
}

fn authority(journal: &ErasureJournal, snapshot: &RecallHistorySnapshot) -> RecallHistoryAuthority {
    RecallHistoryAuthority::for_backup_inspection(
        snapshot.namespace.clone(),
        snapshot.database.clone(),
        journal.identity().into(),
        "named-Å/研发".into(),
        *journal.reference_key_bytes(),
        vec!["entity".into()],
    )
    .unwrap()
}

fn prepare_fixture(journal: &ErasureJournal, proof: &VerifiedBackupOrigins) -> StorageResult<()> {
    journal.begin_restore(&crate::erasure_journal::RestoreMarker {
        snapshot_id: proof.0.snapshot_id.clone(),
        sha256: proof.0.snapshot_sha256.clone(),
    })?;
    journal.with_restore_origins_writer(|| {
        prepare_import_unacknowledged(
            journal,
            proof,
            &snapshot(&proof.0.namespace, &proof.0.database, vec![]),
        )
    })
}

fn bind_fixture(
    journal: &ErasureJournal,
    proof: &VerifiedBackupOrigins,
    actual: &RecallHistorySnapshot,
) -> StorageResult<()> {
    prepare_fixture(journal, proof)?;
    bind_imported(journal, proof, actual)?;
    journal.finish_restore_checked(&crate::erasure_journal::RestoreMarker {
        snapshot_id: proof.0.snapshot_id.clone(),
        sha256: proof.0.snapshot_sha256.clone(),
    })
}

#[test]
fn native_backup_origin_authenticates_original_journal_scope_payload_and_retention() {
    let root = tempfile::tempdir().unwrap();
    let journal = journal(root.path());
    let original = snapshot(
        "ns",
        "db",
        vec![record("ns", "db", u64::MAX - 9, "source-private-canary")],
    );
    let (path, manifest, _) = proof(
        root.path(),
        &journal,
        &original,
        BTreeSet::from(["a".repeat(64)]),
    );
    let bytes = fs::read(path.join(SIDECAR)).unwrap();
    assert!(!bytes
        .windows(b"source-private-canary".len())
        .any(|part| part == b"source-private-canary"));
    assert!(!String::from_utf8(bytes.clone())
        .unwrap()
        .contains(&hex::encode(journal.reference_key_bytes())));
    assert!(!String::from_utf8(bytes)
        .unwrap()
        .contains("entity:original"));
    assert!(verify_backup(&journal, &path, &manifest, "other", "db").is_err());
    let foreign = ErasureJournal::open(root.path().join("foreign-journal")).unwrap();
    assert!(verify_backup(&foreign, &path, &manifest, "ns", "db").is_err());
    for mutation in [0, 1, 2] {
        let mut altered = manifest.clone();
        match mutation {
            0 => altered.sha256 = "f".repeat(64),
            1 => altered.snapshot_id = uuid::Uuid::new_v4().to_string(),
            _ => altered.expires_at += chrono::Duration::hours(1),
        }
        assert!(verify_backup(&journal, &path, &altered, "ns", "db").is_err());
    }
}

#[test]
fn native_backup_origin_refuses_tampering_duplicate_tokens_and_noncanonical_bytes() {
    let root = tempfile::tempdir().unwrap();
    let journal = journal(root.path());
    let original = snapshot("ns", "db", vec![record("ns", "db", 17, "payload")]);
    let (path, manifest, _) = proof(
        root.path(),
        &journal,
        &original,
        BTreeSet::from(["a".repeat(64)]),
    );
    let bytes = fs::read(path.join(SIDECAR)).unwrap();
    let original_text = String::from_utf8(bytes.clone()).unwrap();
    for changed in [
        format!("{original_text}\n"),
        original_text.replace(
            &format!("[\"{}\"]", "a".repeat(64)),
            &format!("[\"{}\",\"{}\"]", "a".repeat(64), "a".repeat(64)),
        ),
        original_text.replace(&"a".repeat(64), &"b".repeat(64)),
    ] {
        fs::write(path.join(SIDECAR), changed).unwrap();
        assert!(verify_backup(&journal, &path, &manifest, "ns", "db").is_err());
    }
    fs::write(path.join(SIDECAR), bytes).unwrap();
    assert!(verify_backup(&journal, &path, &manifest, "ns", "db").is_ok());
}

#[test]
fn native_import_origin_requires_exact_version_and_preserves_historical_marker_changes() {
    let root = tempfile::tempdir().unwrap();
    let journal = journal(root.path());
    let original = snapshot(
        "ns",
        "db",
        vec![record("ns", "db", 11, "same independent text")],
    );
    let (_, _, proof) = proof(
        root.path(),
        &journal,
        &original,
        BTreeSet::from(["c".repeat(64)]),
    );
    let imported = record("ns", "db", 1001, "same independent text");
    let imported_snapshot = snapshot("ns", "db", vec![imported.clone()]);
    bind_fixture(&journal, &proof, &imported_snapshot).unwrap();
    let mut historical = imported;
    historical.current = false;
    let independent_b = record("ns", "db", 1002, "same independent text");
    let next = snapshot("ns", "db", vec![historical.clone(), independent_b.clone()]);
    let authority = authority(&journal, &next);
    let actual = restored_history_causality(&journal, &next, &authority).unwrap();
    assert_eq!(actual.len(), 1);
    assert_eq!(
        actual.get(&authority.version_commitment(&historical).unwrap()),
        Some(&BTreeSet::from(["c".repeat(64)]))
    );
    assert!(!actual.contains_key(&authority.version_commitment(&independent_b).unwrap()));
    let mut changed = historical;
    changed.timestamp -= 1;
    let wrong = snapshot("ns", "db", vec![changed, independent_b]);
    assert!(restored_history_causality(&journal, &wrong, &authority)
        .unwrap()
        .is_empty());
}

#[test]
fn native_import_origin_rejects_missing_extra_or_changed_rows_before_binding_success() {
    let root = tempfile::tempdir().unwrap();
    let journal = journal(root.path());
    let original = snapshot("ns", "db", vec![record("ns", "db", 11, "payload")]);
    let (_, _, proof) = proof(root.path(), &journal, &original, BTreeSet::new());
    for actual in [
        snapshot("ns", "db", vec![]),
        snapshot("ns", "db", vec![record("ns", "db", 20, "changed")]),
        snapshot("other", "db", vec![record("other", "db", 20, "payload")]),
    ] {
        assert!(bind_fixture(&journal, &proof, &actual).is_err());
        assert!(journal.restore_origins_required().unwrap());
        let state = load_imported(&journal).unwrap().unwrap();
        assert!(state.scopes[0].rows.is_empty());
        assert!(state.scopes[0].pending_import.is_some());
    }
}

#[test]
fn native_import_origin_keeps_empty_imported_decision_coverage_and_prior_imports() {
    let root = tempfile::tempdir().unwrap();
    let journal = journal(root.path());
    let original = snapshot("ns", "db", vec![record("ns", "db", 11, "independent")]);
    let (_, _, proof) = proof(root.path(), &journal, &original, BTreeSet::new());
    for version in [101, 102] {
        bind_fixture(
            &journal,
            &proof,
            &snapshot("ns", "db", vec![record("ns", "db", version, "independent")]),
        )
        .unwrap();
    }
    let mut historical = record("ns", "db", 101, "independent");
    historical.current = false;
    let actual = snapshot(
        "ns",
        "db",
        vec![historical, record("ns", "db", 102, "independent")],
    );
    let map = restored_history_causality(&journal, &actual, &authority(&journal, &actual)).unwrap();
    assert_eq!(map.len(), 2);
    assert!(map.values().all(BTreeSet::is_empty));
    let (_, _, reexport) = proof_fixture_again(root.path(), &journal, &actual);
    assert_eq!(reexport.0.rows[0].original_version, 11);
}

fn proof_fixture_again(
    root: &Path,
    journal: &ErasureJournal,
    actual: &RecallHistorySnapshot,
) -> (std::path::PathBuf, SnapshotManifest, VerifiedBackupOrigins) {
    proof(root, journal, actual, BTreeSet::new())
}

#[test]
fn native_import_origin_never_uses_zero_version_as_persistent_content_authority() {
    let root = tempfile::tempdir().unwrap();
    let journal = journal(root.path());
    let original = snapshot("ns", "db", vec![record("ns", "db", 0, "independent")]);
    let (_, _, proof) = proof(root.path(), &journal, &original, BTreeSet::new());
    assert!(matches!(
        proof.0.rows[0].mode,
        OriginMode::VersionlessCurrent
    ));
    bind_fixture(&journal, &proof, &original).unwrap();
    assert!(journal.restore_origins_required().unwrap());
    assert!(
        restored_history_causality(&journal, &original, &authority(&journal, &original))
            .unwrap()
            .is_empty()
    );
    assert!(load_imported(&journal).unwrap().unwrap().scopes[0]
        .rows
        .is_empty());
}

#[test]
fn native_import_origin_independent_handles_refuse_overlap_then_union_all_scopes() {
    let root = tempfile::tempdir().unwrap();
    let first = journal(root.path());
    let second = ErasureJournal::open(first.path()).unwrap();
    let original_a = snapshot("first", "db", vec![record("first", "db", 9, "a")]);
    let original_b = snapshot("second", "db", vec![record("second", "db", 9, "b")]);
    let (_, _, proof_a) = proof(root.path(), &first, &original_a, BTreeSet::new());
    let (_, _, proof_b) = proof(root.path(), &second, &original_b, BTreeSet::new());
    let (entered, entry) = std::sync::mpsc::channel();
    let (release, released) = std::sync::mpsc::channel();
    let task = std::thread::spawn(move || {
        first
            .with_restore_origins_writer(|| {
                entered.send(()).unwrap();
                released
                    .recv_timeout(std::time::Duration::from_secs(5))
                    .unwrap();
                prepare_import_unacknowledged(&first, &proof_a, &snapshot("first", "db", vec![]))?;
                bind_imported_unacknowledged(&first, &proof_a, &original_a)
            })
            .unwrap();
    });
    entry
        .recv_timeout(std::time::Duration::from_secs(5))
        .unwrap();
    assert!(second
        .with_restore_origins_writer(|| {
            prepare_import_unacknowledged(&second, &proof_b, &snapshot("second", "db", vec![]))?;
            bind_imported_unacknowledged(&second, &proof_b, &original_b)
        })
        .is_err());
    release.send(()).unwrap();
    task.join().unwrap();
    second
        .with_restore_origins_writer(|| {
            prepare_import_unacknowledged(&second, &proof_b, &snapshot("second", "db", vec![]))?;
            bind_imported_unacknowledged(&second, &proof_b, &original_b)
        })
        .unwrap();
    let reopened = ErasureJournal::open(second.path()).unwrap();
    let state = load_imported(&reopened).unwrap().unwrap();
    assert_eq!(state.scopes.len(), 2);
    assert_eq!(
        (
            state.scopes[0].namespace.as_str(),
            state.scopes[1].namespace.as_str()
        ),
        ("first", "second")
    );
    assert!(state.scopes.iter().all(|scope| scope.rows.len() == 1));
}

#[test]
fn native_import_origin_file_before_ack_resumes_and_acknowledged_loss_blocks_open() {
    let root = tempfile::tempdir().unwrap();
    let journal = journal(root.path());
    let original = snapshot("ns", "db", vec![record("ns", "db", 11, "payload")]);
    let (_, _, proof) = proof(root.path(), &journal, &original, BTreeSet::new());
    journal
        .begin_restore(&crate::erasure_journal::RestoreMarker {
            snapshot_id: proof.0.snapshot_id.clone(),
            sha256: proof.0.snapshot_sha256.clone(),
        })
        .unwrap();
    assert!(journal
        .with_restore_origins_writer(|| {
            prepare_import_unacknowledged(&journal, &proof, &snapshot("ns", "db", vec![]))?;
            Err(io_error(
                "injected crash after durable origin file before index ACK",
            ))
        })
        .is_err());
    assert!(!journal.restore_origins_required().unwrap());
    let reopened = ErasureJournal::open(journal.path()).unwrap();
    bind_fixture(&reopened, &proof, &original).unwrap();
    assert!(reopened.restore_origins_required().unwrap());
    let bytes = fs::read(journal.path().join(IMPORTED)).unwrap();
    fs::write(journal.path().join(IMPORTED), b"{}").unwrap();
    assert!(ErasureJournal::open(journal.path()).is_err());
    fs::write(journal.path().join(IMPORTED), bytes).unwrap();
    assert!(ErasureJournal::open(journal.path()).is_ok());
    fs::remove_file(journal.path().join(IMPORTED)).unwrap();
    assert!(ErasureJournal::open(journal.path()).is_err());
}

#[cfg(unix)]
#[test]
fn native_backup_origin_refuses_symlinks_hardlinks_public_mode_and_oversize() {
    use std::os::unix::fs::{symlink, PermissionsExt};
    let root = tempfile::tempdir().unwrap();
    let journal = journal(root.path());
    let original = snapshot("ns", "db", vec![record("ns", "db", 11, "payload")]);
    let (path, manifest, _) = proof(root.path(), &journal, &original, BTreeSet::new());
    let file = path.join(SIDECAR);
    let bytes = fs::read(&file).unwrap();
    fs::set_permissions(&file, fs::Permissions::from_mode(0o644)).unwrap();
    assert!(verify_backup(&journal, &path, &manifest, "ns", "db").is_err());
    fs::set_permissions(&file, fs::Permissions::from_mode(0o600)).unwrap();
    let alias = root.path().join("alias");
    symlink(&path, &alias).unwrap();
    assert!(verify_backup(&journal, &alias, &manifest, "ns", "db").is_err());
    let hardlink = root.path().join("hardlink");
    fs::hard_link(&file, &hardlink).unwrap();
    assert!(verify_backup(&journal, &path, &manifest, "ns", "db").is_err());
    fs::remove_file(hardlink).unwrap();
    File::options()
        .write(true)
        .open(&file)
        .unwrap()
        .set_len(MAX_BYTES as u64 + 1)
        .unwrap();
    assert!(verify_backup(&journal, &path, &manifest, "ns", "db").is_err());
    fs::write(&file, bytes).unwrap();
    assert!(verify_backup(&journal, &path, &manifest, "ns", "db").is_ok());
}

#[test]
fn native_backup_origin_refuses_missing_causal_coverage_and_bounded_overflow() {
    let root = tempfile::tempdir().unwrap();
    let journal = journal(root.path());
    let original = snapshot("ns", "db", vec![record("ns", "db", 11, "payload")]);
    let output = root.path().join("proof");
    private_directory(&output).unwrap();
    assert!(write_backup(
        &journal,
        &output,
        &manifest(&journal),
        &original,
        BTreeMap::new()
    )
    .is_err());
    let row = Origin {
        content: "a".repeat(64),
        mode: OriginMode::NativeVersions,
        original_version: 1,
        original_timestamp: 2,
        causal_sources: BTreeSet::new(),
    };
    assert!(origin_shape(std::iter::repeat_n(&row, MAX_RECORDS + 1)).is_err());
    assert!(!output.join(SIDECAR).exists());
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
async fn native_backup_origin_tamper_refuses_before_live_replacement_or_restore_marker() {
    let root = tempfile::tempdir().unwrap();
    let client = StorageClient::new_with_durability(
        StorageConfig::default(),
        None,
        Some(root.path().join("journal")),
    )
    .await
    .unwrap();
    client
        .query_checked("CREATE independent:one SET space_id='named-space', value='before'")
        .await
        .unwrap();
    let output = root.path().join("backup");
    crate::snapshot::export_named(
        &client,
        &output,
        "original",
        &crate::snapshot::SnapshotRetention::default(),
    )
    .await
    .unwrap();
    client
        .query_checked("UPDATE independent:one SET value='after'")
        .await
        .unwrap();
    let independent = ErasureJournal::open(client.erasure_journal().unwrap().path()).unwrap();
    let owner = crate::snapshot::restore_ownership(&independent).unwrap();
    assert!(
        crate::snapshot::restore_named(&client, &output, client.erasure_journal().unwrap())
            .await
            .is_err()
    );
    assert!(client
        .erasure_journal()
        .unwrap()
        .restore_marker()
        .unwrap()
        .is_none());
    assert!(client.recovery_status().ready);
    drop(owner);
    let mut proof: Signed<BackupOrigins> = read(&output.join(SIDECAR)).unwrap();
    proof.authentication = "0".repeat(64);
    write_private_configuration(&output.join(SIDECAR), &encode(&proof).unwrap()).unwrap();
    assert!(
        crate::snapshot::restore_named(&client, &output, client.erasure_journal().unwrap())
            .await
            .is_err()
    );
    assert!(client
        .erasure_journal()
        .unwrap()
        .restore_marker()
        .unwrap()
        .is_none());
    assert!(client.recovery_status().ready);
    let mut response = client
        .query_checked("SELECT VALUE value FROM independent:one")
        .await
        .unwrap();
    assert_eq!(response.take::<Vec<String>>(0).unwrap(), vec!["after"]);
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
async fn native_backup_origin_retention_deletes_only_exact_registered_sidecar() {
    let root = tempfile::tempdir().unwrap();
    let client = StorageClient::new_with_durability(
        StorageConfig::default(),
        None,
        Some(root.path().join("journal")),
    )
    .await
    .unwrap();
    let output = root.path().join("backup");
    let manifest = crate::snapshot::export_named(
        &client,
        &output,
        "original",
        &crate::snapshot::SnapshotRetention::default(),
    )
    .await
    .unwrap();
    assert!(output.join(SIDECAR).is_file());
    let journal = client.erasure_journal().unwrap();
    let registry = journal
        .backups_path()
        .join(format!("{}.json", manifest.snapshot_id));
    let mut registered: serde_json::Value =
        serde_json::from_slice(&fs::read(&registry).unwrap()).unwrap();
    registered["expires_at"] =
        serde_json::to_value(Utc::now() - chrono::Duration::seconds(1)).unwrap();
    fs::write(&registry, serde_json::to_vec(&registered).unwrap()).unwrap();
    let unrelated = root.path().join("operator-file");
    fs::write(&unrelated, b"keep").unwrap();
    assert_eq!(crate::snapshot::cleanup_expired(journal).unwrap(), 1);
    assert!(!output.exists());
    assert_eq!(fs::read(unrelated).unwrap(), b"keep");
}

#[test]
fn native_import_origin_resume_keeps_original_floor_and_captures_partial_retry_history() {
    let root = tempfile::tempdir().unwrap();
    let journal = journal(root.path());
    let original = snapshot("ns", "db", vec![record("ns", "db", 11, "identical text")]);
    let (_, _, proof) = proof(
        root.path(),
        &journal,
        &original,
        BTreeSet::from(["d".repeat(64)]),
    );
    journal
        .begin_restore(&crate::erasure_journal::RestoreMarker {
            snapshot_id: proof.0.snapshot_id.clone(),
            sha256: proof.0.snapshot_sha256.clone(),
        })
        .unwrap();
    let preexisting_b = record("ns", "db", 7, "identical text");
    journal
        .with_restore_origins_writer(|| {
            prepare_import_unacknowledged(
                &journal,
                &proof,
                &snapshot("ns", "db", vec![preexisting_b.clone()]),
            )
        })
        .unwrap();
    let mut old_b = preexisting_b;
    old_b.current = false;
    let mut first_import = record("ns", "db", 8, "identical text");
    let interrupted = snapshot("ns", "db", vec![old_b.clone(), first_import.clone()]);
    // Simulate a crash after import, before the completion binding. Resume uses
    // the first floor, then durably saves this partial import before retrying.
    let reopened = ErasureJournal::open(journal.path()).unwrap();
    reopened
        .with_restore_origins_writer(|| {
            prepare_import_unacknowledged(&reopened, &proof, &interrupted)
        })
        .unwrap();
    let state = load_imported(&reopened).unwrap().unwrap();
    assert_eq!(
        state.scopes[0]
            .pending_import
            .as_ref()
            .unwrap()
            .max_preimport_version,
        7
    );
    assert_eq!(state.scopes[0].rows.len(), 1);
    assert_eq!(state.scopes[0].rows[0].imported_version, 8);
    first_import.current = false;
    let retry = record("ns", "db", 9, "identical text");
    let completed = snapshot(
        "ns",
        "db",
        vec![old_b.clone(), first_import.clone(), retry.clone()],
    );
    bind_imported(&reopened, &proof, &completed).unwrap();
    let authority = authority(&reopened, &completed);
    let actual = restored_history_causality(&reopened, &completed, &authority).unwrap();
    assert_eq!(actual.len(), 2);
    assert!(actual.contains_key(&authority.version_commitment(&first_import).unwrap()));
    assert!(actual.contains_key(&authority.version_commitment(&retry).unwrap()));
    assert!(!actual.contains_key(&authority.version_commitment(&old_b).unwrap()));
    assert!(load_imported(&reopened).unwrap().unwrap().scopes[0]
        .pending_import
        .is_none());
}

#[test]
fn native_import_origin_cannot_reset_pending_floor_or_certify_preexisting_equal_rows() {
    let root = tempfile::tempdir().unwrap();
    let journal = journal(root.path());
    let original = snapshot("ns", "db", vec![record("ns", "db", 11, "identical text")]);
    let (_, _, first) = proof(root.path(), &journal, &original, BTreeSet::new());
    let (_, _, replacement) = proof(root.path(), &journal, &original, BTreeSet::new());
    let before = snapshot("ns", "db", vec![record("ns", "db", 7, "identical text")]);
    journal
        .with_restore_origins_writer(|| prepare_import_unacknowledged(&journal, &first, &before))
        .unwrap();
    let bytes = fs::read(journal.path().join(IMPORTED)).unwrap();
    assert!(bind_imported(&journal, &first, &before).is_err());
    assert!(journal
        .with_restore_origins_writer(|| prepare_import_unacknowledged(
            &journal,
            &replacement,
            &before
        ))
        .is_err());
    assert_eq!(fs::read(journal.path().join(IMPORTED)).unwrap(), bytes);
}
