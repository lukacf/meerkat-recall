//! Original-journal custody for opaque native erasure references.
//!
//! These files live outside database snapshots and payload checkpoint rotation.
//! An acknowledged membership write is monotonic and independently durable.

use super::{ErasureIntent, ErasureJournal};
use crate::durability_io::{atomic_write, io_error, private_directory};
use crate::{StorageError, StorageResult};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

const KEY_FILE: &str = "reference-key.json";
const FINGERPRINT_FILE: &str = "reference-key-fingerprint";
const INDEX_FILE: &str = "reference-membership-index.json";
const MEMBERSHIP_DIRECTORY: &str = "reference-memberships";
const FORMAT_VERSION: u32 = 1;

#[allow(
    clippy::trivially_copy_pass_by_ref,
    reason = "Serde skip_serializing_if requires a borrowed field callback."
)]
const fn is_false(value: &bool) -> bool {
    !*value
}

#[derive(Clone)]
pub(super) struct ReferenceKey {
    bytes: [u8; 32],
    fingerprint: String,
}

impl std::fmt::Debug for ReferenceKey {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("ReferenceKey([REDACTED])")
    }
}

// Deliberately no Debug: deserialization errors must not print the key either.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct KeyFile {
    version: u32,
    journal_identity: String,
    key: String,
    fingerprint: String,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct MembershipIndex {
    version: u32,
    journal_identity: String,
    key_fingerprint: String,
    intents: BTreeSet<String>,
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    history_intents: BTreeSet<String>,
    #[serde(default, skip_serializing_if = "is_false")]
    restore_origins_required: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    operational_archive_registry_sha256: Option<String>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Membership {
    version: u32,
    journal_identity: String,
    key_fingerprint: String,
    intent_id: String,
    intent_digest: String,
    tokens: BTreeSet<String>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CheckedFile<T> {
    payload: T,
    checksum: String,
}

fn checksum<T: Serialize>(payload: &T) -> StorageResult<String> {
    let mut hasher = Sha256::new();
    hasher.update(b"meerkat-recall/reference-journal/v1\0");
    hasher.update(serde_json::to_vec(payload).map_err(io_error)?);
    Ok(hex::encode(hasher.finalize()))
}

fn key_fingerprint(identity: &str, key: &[u8; 32]) -> StorageResult<String> {
    crate::reference_fence::reference_key_fingerprint(identity, key)
}

fn canonical_uuid(value: &str) -> bool {
    uuid::Uuid::parse_str(value).is_ok_and(|id| id.to_string() == value)
}

fn valid_token(token: &str) -> bool {
    token.len() == 64
        && token
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn private_file_bytes(path: &Path) -> StorageResult<Vec<u8>> {
    let metadata = fs::symlink_metadata(path).map_err(io_error)?;
    if !metadata.is_file() {
        return Err(io_error(
            "private reference metadata must be a regular file",
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o077 != 0 {
            return Err(io_error(
                "private reference metadata permissions are too broad",
            ));
        }
    }
    fs::read(path).map_err(io_error)
}

// Following a dangling symlink would misclassify corrupt private metadata as
// absent and could regenerate a fresh key or accept an empty pending membership.
fn entry_exists(path: &Path) -> StorageResult<bool> {
    match fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(io_error(error)),
    }
}

fn write_checked<T: Serialize>(path: &Path, payload: T) -> StorageResult<()> {
    let checksum = checksum(&payload)?;
    atomic_write(
        path,
        &serde_json::to_vec(&CheckedFile { payload, checksum }).map_err(io_error)?,
    )
}

fn read_checked<T: Serialize + serde::de::DeserializeOwned>(path: &Path) -> StorageResult<T> {
    let file: CheckedFile<T> = serde_json::from_slice(&private_file_bytes(path)?)
        .map_err(|_| io_error("invalid private reference metadata"))?;
    if file.checksum != checksum(&file.payload)? {
        return Err(io_error("private reference metadata checksum mismatch"));
    }
    Ok(file.payload)
}

fn read_key(root: &Path) -> StorageResult<(String, ReferenceKey)> {
    let file: KeyFile = serde_json::from_slice(&private_file_bytes(&root.join(KEY_FILE))?)
        .map_err(|_| io_error("invalid private reference key metadata"))?;
    let mut bytes = [0; 32];
    if file.version != FORMAT_VERSION
        || !canonical_uuid(&file.journal_identity)
        || !valid_token(&file.key)
        || hex::decode_to_slice(&file.key, &mut bytes).is_err()
        || file.fingerprint != key_fingerprint(&file.journal_identity, &bytes)?
    {
        return Err(io_error("private reference key identity mismatch"));
    }
    Ok((
        file.journal_identity,
        ReferenceKey {
            bytes,
            fingerprint: file.fingerprint,
        },
    ))
}

// Called only while holding the original journal initialization OS lock. A
// partial *fresh* initialization may be completed, but an established journal
// can never regenerate a missing key/index/fingerprint.
pub(super) fn open_key(root: &Path) -> StorageResult<(String, ReferenceKey)> {
    let identity_path = root.join("journal-id");
    if entry_exists(&identity_path)? {
        let identity = fs::read_to_string(&identity_path).map_err(io_error)?;
        let (key_identity, key) = read_key(root)?;
        if !canonical_uuid(&identity)
            || key_identity != identity
            || private_file_bytes(&root.join(FINGERPRINT_FILE))? != key.fingerprint.as_bytes()
        {
            return Err(io_error("original journal reference key mismatch"));
        }
        // The index is always created before the established journal anchor.
        validate_index(root, &identity, &key.fingerprint)?;
        return Ok((identity, key));
    }

    ensure_fresh_initialization(root)?;
    let (identity, key) = if entry_exists(&root.join(KEY_FILE))? {
        read_key(root)?
    } else {
        if entry_exists(&root.join(FINGERPRINT_FILE))? || entry_exists(&root.join(INDEX_FILE))? {
            return Err(io_error(
                "incomplete journal lost its original reference key",
            ));
        }
        let identity = uuid::Uuid::new_v4().to_string();
        let mut bytes = [0; 32];
        getrandom::getrandom(&mut bytes)
            .map_err(|_| io_error("operating system reference key generation failed"))?;
        let key = ReferenceKey {
            fingerprint: key_fingerprint(&identity, &bytes)?,
            bytes,
        };
        let file = KeyFile {
            version: FORMAT_VERSION,
            journal_identity: identity.clone(),
            key: hex::encode(bytes),
            fingerprint: key.fingerprint.clone(),
        };
        atomic_write(
            &root.join(KEY_FILE),
            &serde_json::to_vec(&file).map_err(io_error)?,
        )?;
        (identity, key)
    };
    if entry_exists(&root.join(FINGERPRINT_FILE))? {
        if private_file_bytes(&root.join(FINGERPRINT_FILE))? != key.fingerprint.as_bytes() {
            return Err(io_error(
                "incomplete journal reference fingerprint mismatch",
            ));
        }
    } else {
        atomic_write(&root.join(FINGERPRINT_FILE), key.fingerprint.as_bytes())?;
    }
    private_directory(&root.join(MEMBERSHIP_DIRECTORY))?;
    if entry_exists(&root.join(INDEX_FILE))? {
        let index = validate_index(root, &identity, &key.fingerprint)?;
        if !index.intents.is_empty() {
            return Err(io_error("populated membership index has no journal anchor"));
        }
    } else {
        write_checked(
            &root.join(INDEX_FILE),
            MembershipIndex {
                version: FORMAT_VERSION,
                journal_identity: identity.clone(),
                key_fingerprint: key.fingerprint.clone(),
                intents: BTreeSet::new(),
                history_intents: BTreeSet::new(),
                restore_origins_required: false,
                operational_archive_registry_sha256: None,
            },
        )?;
    }
    // Last durable step: its existence means ALL private anchors are required.
    atomic_write(&identity_path, identity.as_bytes())?;
    Ok((identity, key))
}

fn ensure_fresh_initialization(root: &Path) -> StorageResult<()> {
    for entry in fs::read_dir(root).map_err(io_error)? {
        let entry = entry.map_err(io_error)?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if matches!(name.as_ref(), "erasures" | "backups" | MEMBERSHIP_DIRECTORY) {
            if !entry.file_type().map_err(io_error)?.is_dir()
                || fs::read_dir(entry.path())
                    .map_err(io_error)?
                    .next()
                    .is_some()
            {
                return Err(io_error(
                    "populated journal cannot initialize a new reference key",
                ));
            }
        } else if !matches!(
            name.as_ref(),
            KEY_FILE | FINGERPRINT_FILE | INDEX_FILE | "journal-initialization.writer.lock"
        ) && !name.starts_with(".recall-write-")
        {
            return Err(io_error(
                "existing journal requires its original identity and reference key",
            ));
        }
    }
    Ok(())
}

fn validate_index(
    root: &Path,
    identity: &str,
    fingerprint: &str,
) -> StorageResult<MembershipIndex> {
    let index: MembershipIndex = read_checked(&root.join(INDEX_FILE))?;
    if index.version != FORMAT_VERSION
        || index.journal_identity != identity
        || index.key_fingerprint != fingerprint
        || index
            .operational_archive_registry_sha256
            .as_ref()
            .is_some_and(|hash| !valid_token(hash))
        || index
            .intents
            .iter()
            .chain(&index.history_intents)
            .any(|id| !canonical_uuid(id))
    {
        return Err(io_error("reference membership index identity mismatch"));
    }
    Ok(index)
}

impl ErasureJournal {
    pub(super) fn acknowledged_history_intents(&self) -> StorageResult<BTreeSet<String>> {
        Ok(
            validate_index(&self.root, &self.identity, self.reference_key_fingerprint())?
                .history_intents,
        )
    }

    // The history writer holds the SAME original reference-membership OS lock.
    // Keeping acknowledgment in this existing mandatory anchor detects a lost
    // complete history directory as well as an individually missing file.
    pub(super) fn acknowledge_history_intent(&self, intent: &ErasureIntent) -> StorageResult<()> {
        self.validate_intent(intent)?;
        let mut index =
            validate_index(&self.root, &self.identity, self.reference_key_fingerprint())?;
        index.history_intents.insert(intent.intent_id.clone());
        write_checked(&self.root.join(INDEX_FILE), index)
    }

    /// Non-secret identity of the original private reference key, bound to stores.
    #[must_use]
    pub fn reference_key_fingerprint(&self) -> &str {
        &self.reference_key.fingerprint
    }

    /// An imported native-history proof remains mandatory across restarts.
    pub(crate) fn restore_origins_required(&self) -> StorageResult<bool> {
        Ok(
            validate_index(&self.root, &self.identity, self.reference_key_fingerprint())?
                .restore_origins_required,
        )
    }

    /// Serialize the complete signed union read/write and its acknowledgment
    /// under the same original index lock used by both other journal writers.
    pub(crate) fn with_restore_origins_writer(
        &self,
        operation: impl FnOnce() -> StorageResult<()>,
    ) -> StorageResult<()> {
        let _writer = self
            .writer
            .lock()
            .map_err(|_| io_error("journal lock poisoned"))?;
        let _disk_writer = crate::exclusive_store_lock::StoreWriterLock::acquire(
            &self.root.join("reference-membership"),
        )?;
        let mut index =
            validate_index(&self.root, &self.identity, self.reference_key_fingerprint())?;
        operation()?;
        index.restore_origins_required = true;
        write_checked(&self.root.join(INDEX_FILE), index)
    }

    pub(super) fn operational_archive_registry_sha256(&self) -> StorageResult<Option<String>> {
        Ok(
            validate_index(&self.root, &self.identity, self.reference_key_fingerprint())?
                .operational_archive_registry_sha256,
        )
    }

    // Publish a complete immutable registry first, then atomically select its
    // digest in the independently retained mandatory index. A crash before the
    // index update leaves only an unselected payload-free orphan, never a torn
    // acknowledged registry. All writers share the original reference lock.
    pub(super) fn with_operational_archive_writer<T>(
        &self,
        operation: impl FnOnce() -> StorageResult<(T, String)>,
    ) -> StorageResult<T> {
        let _writer = self
            .writer
            .lock()
            .map_err(|_| io_error("journal lock poisoned"))?;
        let _disk_writer = crate::exclusive_store_lock::StoreWriterLock::acquire(
            &self.root.join("reference-membership"),
        )?;
        let mut index =
            validate_index(&self.root, &self.identity, self.reference_key_fingerprint())?;
        let (result, registry_sha256) = operation()?;
        if !valid_token(&registry_sha256) {
            return Err(io_error("archive registry digest differs"));
        }
        index.operational_archive_registry_sha256 = Some(registry_sha256);
        write_checked(&self.root.join(INDEX_FILE), index)?;
        Ok(result)
    }

    /// Trusted query custody only; never serialize into a database or receipt.
    pub(crate) const fn reference_key_bytes(&self) -> &[u8; 32] {
        &self.reference_key.bytes
    }

    /// Durably union all opaque reference tokens BEFORE destructive source purge.
    /// Empty membership is an explicit durable observation, not a missing file.
    pub fn record_reference_membership(
        &self,
        intent: &ErasureIntent,
        tokens: &BTreeSet<String>,
    ) -> StorageResult<()> {
        if tokens.iter().any(|token| !valid_token(token)) {
            return Err(StorageError::DataIntegrityError(
                "reference membership requires lowercase 64-hex opaque tokens".into(),
            ));
        }
        let _writer = self
            .writer
            .lock()
            .map_err(|_| io_error("journal lock poisoned"))?;
        // Separate opens must not lose concurrent union updates either.
        let _disk_writer = crate::exclusive_store_lock::StoreWriterLock::acquire(
            &self.root.join("reference-membership"),
        )?;
        self.validate_intent(intent)?;
        let mut index =
            validate_index(&self.root, &self.identity, self.reference_key_fingerprint())?;
        let mut union = self.reference_membership(intent)?;
        union.extend(tokens.iter().cloned());
        let payload = Membership {
            version: FORMAT_VERSION,
            journal_identity: self.identity.clone(),
            key_fingerprint: self.reference_key_fingerprint().to_owned(),
            intent_id: intent.intent_id.clone(),
            intent_digest: checksum(intent)?,
            tokens: union,
        };
        write_checked(&self.membership_path(intent), payload)?;
        // A lost index publication is recoverable from the complete membership
        // file. Acknowledgment waits for BOTH, so a lost file later fails closed.
        index.intents.insert(intent.intent_id.clone());
        write_checked(&self.root.join(INDEX_FILE), index)
    }

    /// Read the retained monotonic membership for an exact original erasure intent.
    /// An unplanned intent returns empty; a missing acknowledged file is corruption.
    pub fn reference_membership(&self, intent: &ErasureIntent) -> StorageResult<BTreeSet<String>> {
        self.validate_intent(intent)?;
        let index = validate_index(&self.root, &self.identity, self.reference_key_fingerprint())?;
        let path = self.membership_path(intent);
        if !entry_exists(&path)? {
            if index.intents.contains(&intent.intent_id) {
                return Err(io_error("acknowledged reference membership is missing"));
            }
            return Ok(BTreeSet::new());
        }
        let membership: Membership = read_checked(&path)?;
        if membership.version != FORMAT_VERSION
            || membership.journal_identity != self.identity
            || membership.key_fingerprint != self.reference_key_fingerprint()
            || membership.intent_id != intent.intent_id
            || membership.intent_digest != checksum(intent)?
            || membership.tokens.iter().any(|token| !valid_token(token))
        {
            return Err(io_error("reference membership intent identity mismatch"));
        }
        Ok(membership.tokens)
    }

    pub(super) fn validate_intent(&self, intent: &ErasureIntent) -> StorageResult<()> {
        if !canonical_uuid(&intent.intent_id) || !self.intents()?.iter().any(|row| row == intent) {
            return Err(io_error(
                "reference membership requires its original committed intent",
            ));
        }
        Ok(())
    }

    fn membership_path(&self, intent: &ErasureIntent) -> PathBuf {
        self.root
            .join(MEMBERSHIP_DIRECTORY)
            .join(format!("{}.json", intent.intent_id))
    }

    pub(super) fn validate_reference_memberships(&self) -> StorageResult<()> {
        let _disk_writer = crate::exclusive_store_lock::StoreWriterLock::acquire(
            &self.root.join("reference-membership"),
        )?;
        let index = validate_index(&self.root, &self.identity, self.reference_key_fingerprint())?;
        let intents = self.intents()?;
        let mut files = BTreeSet::new();
        for entry in fs::read_dir(self.root.join(MEMBERSHIP_DIRECTORY)).map_err(io_error)? {
            let entry = entry.map_err(io_error)?;
            let name = entry.file_name();
            if name.to_string_lossy().starts_with(".recall-write-") {
                continue;
            }
            let intent = intents
                .iter()
                .find(|intent| name == format!("{}.json", intent.intent_id).as_str())
                .ok_or_else(|| io_error("reference membership has no committed intent"))?;
            self.reference_membership(intent)?;
            files.insert(intent.intent_id.clone());
        }
        if !index.intents.is_subset(&files) {
            return Err(io_error("acknowledged reference membership is missing"));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;
    use elephant_types::{RecordId, TableName};

    fn intent(journal: &ErasureJournal) -> ErasureIntent {
        journal
            .record_intent("private-space", &RecordId::new_v4(TableName::Source))
            .unwrap()
    }

    fn tokens(byte: u8) -> BTreeSet<String> {
        BTreeSet::from([hex::encode([byte; 32])])
    }

    #[test]
    fn failed_restore_origins_operation_never_acknowledges_or_changes_original_index() {
        let directory = tempfile::tempdir().unwrap();
        let journal = ErasureJournal::open(directory.path()).unwrap();
        let before = fs::read(directory.path().join(INDEX_FILE)).unwrap();
        assert!(!journal.restore_origins_required().unwrap());
        assert!(journal
            .with_restore_origins_writer(|| Err(io_error(
                "injected before signed proof publication"
            )))
            .is_err());
        assert!(!journal.restore_origins_required().unwrap());
        assert_eq!(fs::read(directory.path().join(INDEX_FILE)).unwrap(), before);
    }

    #[test]
    fn restore_origins_acknowledgment_preserves_existing_membership_and_history_anchors() {
        let directory = tempfile::tempdir().unwrap();
        let journal = ErasureJournal::open(directory.path()).unwrap();
        let intent = intent(&journal);
        journal
            .record_reference_membership(&intent, &tokens(1))
            .unwrap();
        journal
            .record_history_commitments(&intent, "ns", "db", &hex::encode([9; 32]), &tokens(2))
            .unwrap();
        journal
            .with_restore_origins_writer(|| {
                assert!(!journal.restore_origins_required()?);
                Ok(())
            })
            .unwrap();
        assert!(journal.restore_origins_required().unwrap());
        journal
            .record_reference_membership(&intent, &tokens(3))
            .unwrap();
        assert!(journal.restore_origins_required().unwrap());
        assert_eq!(
            journal.reference_membership(&intent).unwrap(),
            tokens(1).union(&tokens(3)).cloned().collect()
        );
        assert_eq!(
            journal.history_commitments(&intent, "ns", "db").unwrap(),
            tokens(2)
        );
        // Startup validation of the actual signed proof is owned by the restore
        // helper's tests. This control isolates the original index transaction.
    }

    #[test]
    fn original_key_survives_restart_and_debug_never_contains_key() {
        let directory = tempfile::tempdir().unwrap();
        let journal = ErasureJournal::open(directory.path()).unwrap();
        let key = *journal.reference_key_bytes();
        let fingerprint = journal.reference_key_fingerprint().to_owned();
        let identity = journal.identity().to_owned();
        let debug = format!("{journal:?}");
        assert!(!debug.contains(&hex::encode(key)));
        assert!(!debug.contains(&format!("{key:?}")));
        drop(journal);
        let reopened = ErasureJournal::open(directory.path()).unwrap();
        assert_eq!(reopened.reference_key_bytes(), &key);
        assert_eq!(reopened.reference_key_fingerprint(), fingerprint);
        assert_eq!(reopened.identity(), identity);
    }

    #[test]
    fn each_established_private_anchor_is_required_even_without_erasures() {
        for name in [KEY_FILE, FINGERPRINT_FILE, INDEX_FILE] {
            let directory = tempfile::tempdir().unwrap();
            let journal = ErasureJournal::open(directory.path()).unwrap();
            let identity = journal.identity().to_owned();
            fs::remove_file(directory.path().join(name)).unwrap();
            assert!(
                ErasureJournal::open(directory.path()).is_err(),
                "missing {name}"
            );
            assert!(
                !directory.path().join(name).exists(),
                "must not regenerate {name}"
            );
            assert_eq!(
                fs::read_to_string(directory.path().join("journal-id")).unwrap(),
                identity
            );
        }
    }

    #[test]
    fn legacy_or_populated_journal_never_silently_generates_new_key() {
        let directory = tempfile::tempdir().unwrap();
        atomic_write(
            &directory.path().join("journal-id"),
            uuid::Uuid::new_v4().to_string().as_bytes(),
        )
        .unwrap();
        assert!(ErasureJournal::open(directory.path()).is_err());
        assert!(!directory.path().join(KEY_FILE).exists());

        let directory = tempfile::tempdir().unwrap();
        private_directory(&directory.path().join("erasures")).unwrap();
        atomic_write(&directory.path().join("erasures/committed.json"), b"{}").unwrap();
        assert!(ErasureJournal::open(directory.path()).is_err());
        assert!(!directory.path().join(KEY_FILE).exists());
    }

    #[test]
    fn fresh_initialization_restarts_after_each_pre_anchor_publication() {
        for stage in 0..3 {
            let directory = tempfile::tempdir().unwrap();
            let key = [0xab; 32];
            let identity = uuid::Uuid::new_v4().to_string();
            let fingerprint = key_fingerprint(&identity, &key).unwrap();
            let file = KeyFile {
                version: FORMAT_VERSION,
                journal_identity: identity.clone(),
                key: hex::encode(key),
                fingerprint: fingerprint.clone(),
            };
            atomic_write(
                &directory.path().join(KEY_FILE),
                &serde_json::to_vec(&file).unwrap(),
            )
            .unwrap();
            if stage >= 1 {
                atomic_write(
                    &directory.path().join(FINGERPRINT_FILE),
                    fingerprint.as_bytes(),
                )
                .unwrap();
            }
            if stage >= 2 {
                write_checked(
                    &directory.path().join(INDEX_FILE),
                    MembershipIndex {
                        version: FORMAT_VERSION,
                        journal_identity: identity.clone(),
                        key_fingerprint: fingerprint.clone(),
                        intents: BTreeSet::new(),
                        history_intents: BTreeSet::new(),
                        restore_origins_required: false,
                        operational_archive_registry_sha256: None,
                    },
                )
                .unwrap();
            }
            let journal = ErasureJournal::open(directory.path()).unwrap();
            assert_eq!(journal.identity(), identity);
            assert_eq!(journal.reference_key_bytes(), &key);
            assert_eq!(journal.reference_key_fingerprint(), fingerprint);
        }
    }

    #[test]
    fn mismatched_key_file_or_fingerprint_blocks_restart() {
        for corrupt_key in [true, false] {
            let directory = tempfile::tempdir().unwrap();
            let journal = ErasureJournal::open(directory.path()).unwrap();
            if corrupt_key {
                let mut file: KeyFile =
                    serde_json::from_slice(&fs::read(directory.path().join(KEY_FILE)).unwrap())
                        .unwrap();
                file.key = hex::encode([0x42; 32]);
                file.fingerprint = key_fingerprint(&file.journal_identity, &[0x42; 32]).unwrap();
                atomic_write(
                    &directory.path().join(KEY_FILE),
                    &serde_json::to_vec(&file).unwrap(),
                )
                .unwrap();
            } else {
                atomic_write(
                    &directory.path().join(FINGERPRINT_FILE),
                    b"wrong fingerprint",
                )
                .unwrap();
            }
            assert!(ErasureJournal::open(directory.path()).is_err());
            assert!(!format!("{journal:?}").contains(&hex::encode(journal.reference_key_bytes())));
        }
    }

    #[test]
    fn membership_is_durable_monotonic_opaque_and_bound_to_exact_intent() {
        let directory = tempfile::tempdir().unwrap();
        let journal = ErasureJournal::open(directory.path()).unwrap();
        let original = intent(&journal);
        assert!(journal.reference_membership(&original).unwrap().is_empty());
        journal
            .record_reference_membership(&original, &tokens(1))
            .unwrap();
        journal
            .record_reference_membership(&original, &tokens(2))
            .unwrap();
        journal
            .record_reference_membership(&original, &BTreeSet::new())
            .unwrap();
        let expected = tokens(1)
            .union(&tokens(2))
            .cloned()
            .collect::<BTreeSet<_>>();
        assert_eq!(journal.reference_membership(&original).unwrap(), expected);
        let file = fs::read_to_string(journal.membership_path(&original)).unwrap();
        assert!(!file.contains(&original.source_id.to_string()));
        assert!(!file.contains(&original.space_id));
        assert!(!file.contains(&hex::encode(journal.reference_key_bytes())));
        let mut forged = original.clone();
        forged.space_id = "other-space".into();
        assert!(journal
            .record_reference_membership(&forged, &tokens(3))
            .is_err());
        drop(journal);
        let reopened = ErasureJournal::open(directory.path()).unwrap();
        assert_eq!(reopened.reference_membership(&original).unwrap(), expected);
    }

    #[test]
    fn membership_rejects_raw_references_noncanonical_tokens_and_unknown_intents() {
        let directory = tempfile::tempdir().unwrap();
        let journal = ErasureJournal::open(directory.path()).unwrap();
        let original = intent(&journal);
        for token in [
            "extension:secret".to_owned(),
            "A".repeat(64),
            "f".repeat(63),
            "g".repeat(64),
        ] {
            assert!(journal
                .record_reference_membership(&original, &BTreeSet::from([token]))
                .is_err());
        }
        assert!(!journal.membership_path(&original).exists());
        let mut unknown = original;
        unknown.intent_id = uuid::Uuid::new_v4().to_string();
        assert!(journal
            .record_reference_membership(&unknown, &tokens(1))
            .is_err());
    }

    #[test]
    fn acknowledged_empty_membership_is_required_after_restart() {
        let directory = tempfile::tempdir().unwrap();
        let journal = ErasureJournal::open(directory.path()).unwrap();
        let original = intent(&journal);
        journal
            .record_reference_membership(&original, &BTreeSet::new())
            .unwrap();
        fs::remove_file(journal.membership_path(&original)).unwrap();
        assert!(journal.reference_membership(&original).is_err());
        assert!(ErasureJournal::open(directory.path()).is_err());
    }

    #[test]
    fn membership_crash_before_index_publication_recovers_complete_union() {
        let directory = tempfile::tempdir().unwrap();
        let journal = ErasureJournal::open(directory.path()).unwrap();
        let original = intent(&journal);
        let old_index = fs::read(directory.path().join(INDEX_FILE)).unwrap();
        journal
            .record_reference_membership(&original, &tokens(7))
            .unwrap();
        // State at a crash between durable membership rename and index rename.
        atomic_write(&directory.path().join(INDEX_FILE), &old_index).unwrap();
        drop(journal);
        let reopened = ErasureJournal::open(directory.path()).unwrap();
        assert_eq!(reopened.reference_membership(&original).unwrap(), tokens(7));
        reopened
            .record_reference_membership(&original, &tokens(8))
            .unwrap();
        fs::remove_file(reopened.membership_path(&original)).unwrap();
        assert!(ErasureJournal::open(directory.path()).is_err());
    }

    #[test]
    fn damaged_or_foreign_membership_blocks_readiness() {
        for foreign in [false, true] {
            let directory = tempfile::tempdir().unwrap();
            let journal = ErasureJournal::open(directory.path()).unwrap();
            let original = intent(&journal);
            journal
                .record_reference_membership(&original, &tokens(3))
                .unwrap();
            if foreign {
                let mut payload: Membership =
                    read_checked(&journal.membership_path(&original)).unwrap();
                payload.journal_identity = uuid::Uuid::new_v4().to_string();
                write_checked(&journal.membership_path(&original), payload).unwrap();
            } else {
                atomic_write(&journal.membership_path(&original), b"{\"payload\":").unwrap();
            }
            assert!(ErasureJournal::open(directory.path()).is_err());
        }
    }

    #[cfg(unix)]
    #[test]
    fn private_key_and_memberships_have_restrictive_permissions() {
        use std::os::unix::fs::PermissionsExt;
        let directory = tempfile::tempdir().unwrap();
        let journal = ErasureJournal::open(directory.path()).unwrap();
        let original = intent(&journal);
        journal
            .record_reference_membership(&original, &tokens(1))
            .unwrap();
        for path in [
            directory.path().join(KEY_FILE),
            directory.path().join(FINGERPRINT_FILE),
            directory.path().join(INDEX_FILE),
            journal.membership_path(&original),
        ] {
            assert_eq!(
                fs::metadata(path).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }
        for path in [
            directory.path().to_path_buf(),
            directory.path().join(MEMBERSHIP_DIRECTORY),
        ] {
            assert_eq!(
                fs::metadata(path).unwrap().permissions().mode() & 0o777,
                0o700
            );
        }
        fs::set_permissions(
            directory.path().join(KEY_FILE),
            fs::Permissions::from_mode(0o644),
        )
        .unwrap();
        assert!(ErasureJournal::open(directory.path()).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn private_key_symlink_is_refused() {
        let directory = tempfile::tempdir().unwrap();
        ErasureJournal::open(directory.path()).unwrap();
        fs::rename(
            directory.path().join(KEY_FILE),
            directory.path().join("moved-key"),
        )
        .unwrap();
        std::os::unix::fs::symlink(
            directory.path().join("moved-key"),
            directory.path().join(KEY_FILE),
        )
        .unwrap();
        assert!(ErasureJournal::open(directory.path()).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn dangling_private_entries_are_corruption_not_fresh_or_empty_state() {
        let fresh = tempfile::tempdir().unwrap();
        std::os::unix::fs::symlink("missing-key", fresh.path().join(KEY_FILE)).unwrap();
        assert!(ErasureJournal::open(fresh.path()).is_err());
        assert!(fs::symlink_metadata(fresh.path().join(KEY_FILE))
            .unwrap()
            .file_type()
            .is_symlink());
        assert!(!fresh.path().join("journal-id").exists());

        let directory = tempfile::tempdir().unwrap();
        let journal = ErasureJournal::open(directory.path()).unwrap();
        let original = intent(&journal);
        std::os::unix::fs::symlink("missing-membership", journal.membership_path(&original))
            .unwrap();
        assert!(journal.reference_membership(&original).is_err());
        assert!(ErasureJournal::open(directory.path()).is_err());
    }
}
