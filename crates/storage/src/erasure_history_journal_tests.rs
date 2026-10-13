#![allow(clippy::expect_used, clippy::unwrap_used)]

use super::*;
use elephant_types::{RecordId, TableName};

fn fixture() -> (tempfile::TempDir, ErasureJournal, ErasureIntent) {
    let directory = tempfile::tempdir().expect("private journal directory");
    let journal = ErasureJournal::open(directory.path()).expect("original journal");
    let intent = journal
        .record_intent("named-space", &RecordId::new_v4(TableName::Source))
        .expect("original intent");
    (directory, journal, intent)
}

fn opaque(byte: u8) -> String {
    hex::encode([byte; 32])
}

fn versions(byte: u8) -> BTreeSet<String> {
    BTreeSet::from([opaque(byte)])
}

fn record(journal: &ErasureJournal, intent: &ErasureIntent, byte: u8) {
    journal
        .record_history_commitments(intent, "namespace", "database", &opaque(9), &versions(byte))
        .expect("durable original history proof");
}

#[test]
fn history_proofs_union_across_reopen_and_independent_reference_index_writes() {
    let (directory, journal, intent) = fixture();
    let second = ErasureJournal::open(directory.path()).expect("separate original handle");
    record(&journal, &intent, 1);
    second
        .record_reference_membership(&intent, &versions(7))
        .expect("existing reference writer must preserve history acknowledgment");
    record(&second, &intent, 2);
    assert_eq!(journal.reference_membership(&intent).unwrap(), versions(7));
    drop(journal);
    drop(second);
    let reopened = ErasureJournal::open(directory.path()).expect("original history reopen");
    assert_eq!(
        reopened
            .history_commitments(&intent, "namespace", "database")
            .unwrap(),
        BTreeSet::from([opaque(1), opaque(2)])
    );
    assert_eq!(
        reopened.acknowledged_history_intents().unwrap(),
        BTreeSet::from([intent.intent_id])
    );
}

#[test]
fn acknowledged_history_file_and_whole_directory_loss_block_startup() {
    for remove_directory in [false, true] {
        let (directory, journal, intent) = fixture();
        record(&journal, &intent, 1);
        let proof_directory = directory.path().join(DIRECTORY);
        fs::remove_file(proof_directory.join(format!("{}.json", intent.intent_id))).unwrap();
        if remove_directory {
            fs::remove_dir(&proof_directory).unwrap();
        }
        drop(journal);
        assert!(ErasureJournal::open(directory.path()).is_err());
        assert!(!proof_directory
            .join(format!("{}.json", intent.intent_id))
            .exists());
    }
}

#[test]
fn complete_unacknowledged_proof_recovers_and_never_loses_its_union() {
    let (directory, journal, intent) = fixture();
    let index_path = directory.path().join("reference-membership-index.json");
    let original_index = fs::read(&index_path).unwrap();
    record(&journal, &intent, 1);
    // The exact durable cut after proof publication, before index publication.
    atomic_write(&index_path, &original_index).unwrap();
    drop(journal);
    let reopened =
        ErasureJournal::open(directory.path()).expect("recover complete unacknowledged proof");
    assert!(reopened.acknowledged_history_intents().unwrap().is_empty());
    assert_eq!(
        reopened
            .history_commitments(&intent, "namespace", "database")
            .unwrap(),
        versions(1)
    );
    record(&reopened, &intent, 2);
    assert_eq!(
        reopened
            .history_commitments(&intent, "namespace", "database")
            .unwrap(),
        BTreeSet::from([opaque(1), opaque(2)])
    );
    assert!(reopened
        .acknowledged_history_intents()
        .unwrap()
        .contains(&intent.intent_id));
}

#[test]
fn explicit_empty_history_proof_is_acknowledged_and_scope_is_not_reused() {
    let (directory, journal, intent) = fixture();
    journal
        .record_history_commitments(
            &intent,
            "namespace",
            "database",
            &opaque(9),
            &BTreeSet::new(),
        )
        .unwrap();
    assert!(journal
        .acknowledged_history_intents()
        .unwrap()
        .contains(&intent.intent_id));
    record(&journal, &intent, 1);
    assert!(journal
        .history_commitments(&intent, "other-namespace", "database")
        .unwrap()
        .is_empty());
    assert!(journal
        .history_commitments(&intent, "namespace", "other-database")
        .unwrap()
        .is_empty());
    drop(journal);
    assert_eq!(
        ErasureJournal::open(directory.path())
            .unwrap()
            .history_commitments(&intent, "namespace", "database")
            .unwrap(),
        versions(1)
    );
}

#[test]
fn history_proofs_bind_original_intent_and_reject_corruption_before_rewrite() {
    let (directory, journal, intent) = fixture();
    record(&journal, &intent, 1);
    let mut altered = intent.clone();
    altered.space_id = "another-space".into();
    assert!(journal
        .record_history_commitments(&altered, "namespace", "database", &opaque(9), &versions(2))
        .is_err());
    let path = directory
        .path()
        .join(DIRECTORY)
        .join(format!("{}.json", intent.intent_id));
    let mut bytes = fs::read(&path).unwrap();
    bytes.pop();
    atomic_write(&path, &bytes).unwrap();
    assert!(journal
        .history_commitments(&intent, "namespace", "database")
        .is_err());
    assert!(journal
        .record_history_commitments(&intent, "namespace", "database", &opaque(9), &versions(2))
        .is_err());
    assert_eq!(fs::read(&path).unwrap(), bytes);
    drop(journal);
    assert!(ErasureJournal::open(directory.path()).is_err());
}

#[test]
fn history_proof_files_contain_only_opaque_scope_and_version_evidence() {
    let (directory, journal, intent) = fixture();
    let namespace = "private_namespace_canary";
    let database = "private_database_canary";
    journal
        .record_history_commitments(&intent, namespace, database, &opaque(9), &versions(1))
        .unwrap();
    let path = directory
        .path()
        .join(DIRECTORY)
        .join(format!("{}.json", intent.intent_id));
    let bytes = fs::read_to_string(path).unwrap();
    assert!(!bytes.contains(namespace));
    assert!(!bytes.contains(database));
    assert!(!bytes.contains(&hex::encode(journal.reference_key_bytes())));
    let checked: CheckedProof = serde_json::from_str(&bytes).unwrap();
    assert_eq!(checked.payload.scopes.len(), 1);
    assert!(checked.payload.scopes.keys().all(|key| token(key)));
    assert!(!bytes.contains("retained_controls"));
    assert!(!bytes.contains("raw_key"));
    assert!(!bytes.contains("native_value"));
}

#[test]
fn original_reference_index_bytes_are_unchanged_until_first_history_acknowledgment() {
    let (directory, journal, intent) = fixture();
    let path = directory.path().join("reference-membership-index.json");
    let before = fs::read(&path).unwrap();
    assert!(!String::from_utf8_lossy(&before).contains("history_intents"));
    assert!(journal
        .history_commitments(&intent, "namespace", "database")
        .unwrap()
        .is_empty());
    drop(journal);
    let reopened = ErasureJournal::open(directory.path()).unwrap();
    assert_eq!(fs::read(&path).unwrap(), before);
    assert!(!directory.path().join(DIRECTORY).exists());
    record(&reopened, &intent, 1);
    assert!(fs::read_to_string(path)
        .unwrap()
        .contains("history_intents"));
}

#[cfg(unix)]
#[test]
fn history_aliases_and_hardlinks_never_qualify_as_original_private_proofs() {
    use std::os::unix::fs::symlink;
    for directory_alias in [false, true] {
        let (directory, journal, intent) = fixture();
        let foreign = tempfile::tempdir().unwrap();
        if directory_alias {
            symlink(foreign.path(), directory.path().join(DIRECTORY)).unwrap();
            assert!(journal
                .record_history_commitments(
                    &intent,
                    "namespace",
                    "database",
                    &opaque(9),
                    &versions(1)
                )
                .is_err());
            assert!(fs::read_dir(foreign.path()).unwrap().next().is_none());
        } else {
            record(&journal, &intent, 1);
            let path = directory
                .path()
                .join(DIRECTORY)
                .join(format!("{}.json", intent.intent_id));
            fs::hard_link(&path, foreign.path().join("alias")).unwrap();
            let original = fs::read(&path).unwrap();
            assert!(journal
                .history_commitments(&intent, "namespace", "database")
                .is_err());
            assert_eq!(fs::read(foreign.path().join("alias")).unwrap(), original);
        }
        drop(journal);
        assert!(ErasureJournal::open(directory.path()).is_err());
    }
}
