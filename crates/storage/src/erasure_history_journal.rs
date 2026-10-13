//! Monotonic, payload-free native history proofs in the original erasure journal.
//!
//! The existing mandatory reference index acknowledges these files; losing the
//! entire history directory cannot silently turn a completed proof into empty.

use super::{ErasureIntent, ErasureJournal};
use crate::durability_io::{atomic_write, io_error, private_directory, sync_directory};
use crate::StorageResult;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::io::Read;
use std::path::Path;

const DIRECTORY: &str = "history-commitments";
const MAX_BYTES: u64 = 128 * 1024 * 1024;
const MAX_COMMITMENTS: usize = 1_000_000;
const MAX_SCOPES: usize = 1024;
const DOMAIN: &[u8] = b"meerkat-recall/history-journal/v1\0";

#[derive(Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ScopeProof {
    history_fingerprints: BTreeSet<String>,
    erased_versions: BTreeSet<String>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct HistoryProof {
    version: u32,
    journal_identity: String,
    key_fingerprint: String,
    intent_id: String,
    intent_digest: String,
    scopes: BTreeMap<String, ScopeProof>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CheckedProof {
    payload: HistoryProof,
    checksum: String,
}

fn digest<T: Serialize>(value: &T) -> StorageResult<String> {
    let mut hash = Sha256::new();
    hash.update(DOMAIN);
    hash.update(serde_json::to_vec(value).map_err(io_error)?);
    Ok(hex::encode(hash.finalize()))
}

fn scope_key(namespace: &str, database: &str) -> StorageResult<String> {
    if namespace.is_empty() || database.is_empty() {
        return Err(io_error("history proof requires exact native scope"));
    }
    // A tuple retains unambiguous field boundaries; names never enter a file.
    digest(&(namespace, database))
}

fn token(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn exists(path: &Path) -> StorageResult<bool> {
    match fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(io_error(error)),
    }
}

fn require_directory(path: &Path) -> StorageResult<()> {
    let metadata = fs::symlink_metadata(path).map_err(io_error)?;
    if !metadata.is_dir() {
        return Err(io_error("history proof directory must not be an alias"));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o077 != 0 {
            return Err(io_error("history proof directory is not private"));
        }
    }
    Ok(())
}

fn read(path: &Path) -> StorageResult<HistoryProof> {
    let metadata = fs::symlink_metadata(path).map_err(io_error)?;
    if !metadata.is_file() || metadata.len() > MAX_BYTES {
        return Err(io_error("history proof file type or size differs"));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        if metadata.permissions().mode() & 0o077 != 0 || metadata.nlink() != 1 {
            return Err(io_error("history proof file is not exclusively private"));
        }
    }
    let mut bytes = Vec::new();
    File::open(path)
        .map_err(io_error)?
        .take(MAX_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(io_error)?;
    if bytes.len() as u64 > MAX_BYTES {
        return Err(io_error("history proof file exceeded its byte bound"));
    }
    let checked: CheckedProof =
        serde_json::from_slice(&bytes).map_err(|_| io_error("invalid history proof metadata"))?;
    if !token(&checked.checksum) || digest(&checked.payload)? != checked.checksum {
        return Err(io_error("history proof checksum differs"));
    }
    Ok(checked.payload)
}

impl ErasureJournal {
    fn empty_history_proof(&self, intent: &ErasureIntent) -> StorageResult<HistoryProof> {
        Ok(HistoryProof {
            version: 1,
            journal_identity: self.identity.clone(),
            key_fingerprint: self.reference_key_fingerprint().into(),
            intent_id: intent.intent_id.clone(),
            intent_digest: digest(intent)?,
            scopes: BTreeMap::new(),
        })
    }

    fn validate_history_proof(
        &self,
        intent: &ErasureIntent,
        proof: &HistoryProof,
    ) -> StorageResult<()> {
        if proof.version != 1
            || proof.journal_identity != self.identity
            || proof.key_fingerprint != self.reference_key_fingerprint()
            || proof.intent_id != intent.intent_id
            || proof.intent_digest != digest(intent)?
            || proof.scopes.len() > MAX_SCOPES
        {
            return Err(io_error("history proof original intent or scope differs"));
        }
        let mut count = 0_usize;
        for (scope, values) in &proof.scopes {
            if !token(scope)
                || values.history_fingerprints.is_empty()
                || values
                    .history_fingerprints
                    .iter()
                    .chain(&values.erased_versions)
                    .any(|value| !token(value))
            {
                return Err(io_error(
                    "history proof contains invalid opaque commitments",
                ));
            }
            count = count
                .checked_add(values.history_fingerprints.len())
                .and_then(|count| count.checked_add(values.erased_versions.len()))
                .ok_or_else(|| io_error("history proof commitment count overflow"))?;
        }
        if count > MAX_COMMITMENTS {
            return Err(io_error("history proof commitment bound exceeded"));
        }
        Ok(())
    }

    fn load_history_proof(&self, intent: &ErasureIntent) -> StorageResult<HistoryProof> {
        self.validate_intent(intent)?;
        let acknowledged = self.acknowledged_history_intents()?;
        let directory = self.root.join(DIRECTORY);
        let path = directory.join(format!("{}.json", intent.intent_id));
        if exists(&directory)? {
            require_directory(&directory)?;
        }
        if !exists(&path)? {
            if acknowledged.contains(&intent.intent_id) {
                return Err(io_error("acknowledged history proof is missing"));
            }
            return self.empty_history_proof(intent);
        }
        let proof = read(&path)?;
        self.validate_history_proof(intent, &proof)?;
        Ok(proof)
    }

    /// Retained erasure commitments only. Current-control exemptions are never
    /// persisted: each inspection must validate its own exact current controls.
    pub(crate) fn history_commitments(
        &self,
        intent: &ErasureIntent,
        namespace: &str,
        database: &str,
    ) -> StorageResult<BTreeSet<String>> {
        let mut proof = self.load_history_proof(intent)?;
        Ok(proof
            .scopes
            .remove(&scope_key(namespace, database)?)
            .map_or_else(BTreeSet::new, |scope| scope.erased_versions))
    }

    /// Publish the monotonic union before any physical generation is replaced.
    /// Success requires the file and its acknowledgment in the original index.
    pub(crate) fn record_history_commitments(
        &self,
        intent: &ErasureIntent,
        namespace: &str,
        database: &str,
        history_fingerprint: &str,
        erased_versions: &BTreeSet<String>,
    ) -> StorageResult<BTreeSet<String>> {
        if !token(history_fingerprint) || erased_versions.iter().any(|value| !token(value)) {
            return Err(io_error("history proof requires exact opaque commitments"));
        }
        let scope = scope_key(namespace, database)?;
        let _writer = self
            .writer
            .lock()
            .map_err(|_| io_error("journal lock poisoned"))?;
        let _disk_writer = crate::exclusive_store_lock::StoreWriterLock::acquire(
            &self.root.join("reference-membership"),
        )?;
        let mut proof = self.load_history_proof(intent)?;
        let values = proof.scopes.entry(scope).or_default();
        values
            .history_fingerprints
            .insert(history_fingerprint.into());
        values
            .erased_versions
            .extend(erased_versions.iter().cloned());
        let union = values.erased_versions.clone();
        self.validate_history_proof(intent, &proof)?;
        let checksum = digest(&proof)?;
        let bytes = serde_json::to_vec(&CheckedProof {
            payload: proof,
            checksum,
        })
        .map_err(|_| io_error("history proof encoding failed"))?;
        if bytes.len() as u64 > MAX_BYTES {
            return Err(io_error("history proof byte bound exceeded"));
        }
        let directory = self.root.join(DIRECTORY);
        if exists(&directory)? {
            require_directory(&directory)?;
        } else {
            private_directory(&directory)?;
            sync_directory(&self.root)?;
        }
        atomic_write(
            &directory.join(format!("{}.json", intent.intent_id)),
            &bytes,
        )?;
        self.acknowledge_history_intent(intent)?;
        Ok(union)
    }

    pub(super) fn validate_history_commitments(&self) -> StorageResult<()> {
        let _disk_writer = crate::exclusive_store_lock::StoreWriterLock::acquire(
            &self.root.join("reference-membership"),
        )?;
        let acknowledged = self.acknowledged_history_intents()?;
        let directory = self.root.join(DIRECTORY);
        if !exists(&directory)? {
            return if acknowledged.is_empty() {
                Ok(())
            } else {
                Err(io_error("acknowledged history directory is missing"))
            };
        }
        require_directory(&directory)?;
        let intents = self.intents()?;
        let mut seen = BTreeSet::new();
        for entry in fs::read_dir(&directory).map_err(io_error)? {
            let entry = entry.map_err(io_error)?;
            let name = entry.file_name();
            if name.to_string_lossy().starts_with(".recall-write-") {
                continue;
            }
            let intent = intents
                .iter()
                .find(|intent| name == format!("{}.json", intent.intent_id).as_str())
                .ok_or_else(|| io_error("history proof has no original committed intent"))?;
            self.load_history_proof(intent)?;
            seen.insert(intent.intent_id.clone());
        }
        if !acknowledged.is_subset(&seen) {
            return Err(io_error("acknowledged history proof is missing"));
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "erasure_history_journal_tests.rs"]
mod tests;
