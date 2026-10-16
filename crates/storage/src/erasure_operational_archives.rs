//! Non-expiring custody for retained operational archive generations.
//! Registry metadata has no original row payload and paths derive only from IDs.
use super::ErasureJournal;
use crate::durability_io::{atomic_write, io_error, sync_directory};
use crate::StorageResult;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};

const REGISTRIES: &str = "operational-archive-registries";
const DIRECTORY: &str = "operational-archives";
const MAX_REGISTRY_BYTES: u64 = 16 * 1024 * 1024;
const MAX_GENERATIONS: usize = 4096;

/// Original immutable source/custody binding reserved before payload creation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArchiveRegistration {
    /// Stable journal-local archive lineage UUID.
    pub lineage_id: String,
    /// Immutable generation UUID, never reused for altered payloads.
    pub generation_id: String,
    /// Exact predecessor generation for a source cut or redaction.
    pub parent_generation_id: Option<String>,
    /// Digest of the core archive format's complete original binding.
    pub binding_sha256: String,
    /// Original snapshot file digest, distinct from the archive digest.
    pub snapshot_sha256: String,
    /// Original independent recovery journal identity.
    pub journal_id: String,
    /// Original source namespace, retained in archived lexical rows.
    pub source_space_id: String,
    /// Explicit destination of the selected source namespace; other row spaces remain unchanged.
    pub target_space_id: String,
}

/// Exact retained seal evidence without treating it as current authority.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArchiveSealEvidence {
    /// Complete source declaration retained for original-generation erasure.
    pub binding: crate::historical_operational_archive::ArchiveBinding,
    /// Exact complete core seal, not only its public file hash.
    pub seal: crate::historical_operational_archive::ArchiveSeal,
    /// Domain-separated digest of the complete core archive seal.
    pub seal_sha256: String,
    /// Hash of the closed immutable archive file.
    pub file_sha256: String,
    /// Exact file byte length.
    pub bytes: u64,
}

/// Durable generation disposition; denied generations never grant read access.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
pub enum ArchiveGenerationState {
    /// Registered path may be incomplete; no read or replay authority exists.
    Pending,
    /// Complete verified bytes still await an exact native completion receipt.
    Sealed {
        /// Complete immutable artifact declaration and seal.
        evidence: ArchiveSealEvidence,
    },
    /// Native completion and retained artifact were bound under original custody.
    Committed {
        /// Complete immutable artifact declaration and seal.
        evidence: ArchiveSealEvidence,
        /// Digest of the original native source-cut control image.
        native_receipt_sha256: String,
    },
    /// Original erasure intent denies reads before any payload is destroyed.
    Redacting {
        /// Complete immutable artifact declaration and seal.
        evidence: ArchiveSealEvidence,
        /// Digest of the original native source-cut control image.
        native_receipt_sha256: String,
        /// Exact independently retained original erasure intent.
        intent_id: String,
        /// Verified successor proof durably selected before unlinking old bytes.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        prepared: Option<ArchiveRedactionProof>,
    },
    /// Exact successor retains unaffected rows; old payload removal is durable.
    Redacted {
        /// Complete immutable artifact declaration and seal.
        evidence: ArchiveSealEvidence,
        /// Digest of the original native source-cut control image.
        native_receipt_sha256: String,
        /// Exact independently retained original erasure intent.
        intent_id: String,
        /// Verified successor generation preserving unaffected rows.
        replacement_generation_id: String,
        /// Exact removed original identity/hash census.
        removed_identity_sha256: String,
        /// Complete surviving original row inventory digest.
        preserved_inventory_sha256: String,
    },
}

/// Exact successor identity recorded before old payload removal begins.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArchiveRedactionProof {
    /// Complete verified successor generation.
    pub replacement_generation_id: String,
    /// Domain-bound exact removed original identities and hashes.
    pub removed_identity_sha256: String,
    /// Complete exact remaining original table/id/hash inventory.
    pub preserved_inventory_sha256: String,
}

/// One exact registry image, used for guarded state transitions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArchiveRegistryEntry {
    /// Immutable original source binding.
    pub registration: ArchiveRegistration,
    /// Current journal-authorized lifecycle state.
    pub state: ArchiveGenerationState,
}

impl ArchiveRegistryEntry {
    pub(crate) fn native_receipt_sha256(&self) -> Option<&str> {
        match &self.state {
            ArchiveGenerationState::Committed {
                native_receipt_sha256,
                ..
            }
            | ArchiveGenerationState::Redacting {
                native_receipt_sha256,
                ..
            }
            | ArchiveGenerationState::Redacted {
                native_receipt_sha256,
                ..
            } => Some(native_receipt_sha256),
            _ => None,
        }
    }
    fn redaction_proof(&self) -> Option<ArchiveRedactionProof> {
        match &self.state {
            ArchiveGenerationState::Redacting { prepared, .. } => prepared.clone(),
            ArchiveGenerationState::Redacted {
                replacement_generation_id,
                removed_identity_sha256,
                preserved_inventory_sha256,
                ..
            } => Some(ArchiveRedactionProof {
                replacement_generation_id: replacement_generation_id.clone(),
                removed_identity_sha256: removed_identity_sha256.clone(),
                preserved_inventory_sha256: preserved_inventory_sha256.clone(),
            }),
            _ => None,
        }
    }
    pub(crate) const fn evidence(&self) -> Option<&ArchiveSealEvidence> {
        match &self.state {
            ArchiveGenerationState::Pending => None,
            ArchiveGenerationState::Sealed { evidence }
            | ArchiveGenerationState::Committed { evidence, .. }
            | ArchiveGenerationState::Redacting { evidence, .. }
            | ArchiveGenerationState::Redacted { evidence, .. } => Some(evidence),
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Registry {
    version: u32,
    journal_id: String,
    key_fingerprint: String,
    entries: BTreeMap<String, ArchiveRegistryEntry>,
    heads: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    discarding_transfer: Option<String>,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CheckedRegistry {
    payload: Registry,
    checksum: String,
}

fn uuid(value: &str) -> bool {
    uuid::Uuid::parse_str(value).is_ok_and(|id| id.to_string() == value)
}
fn hash(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
fn digest(value: &Registry) -> StorageResult<String> {
    let mut h = Sha256::new();
    h.update(b"recall-operational-archive-registry-v1\0");
    h.update(serde_json::to_vec(value).map_err(io_error)?);
    Ok(hex::encode(h.finalize()))
}
fn exists(path: &Path) -> StorageResult<bool> {
    match fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(e) => Err(io_error(e)),
    }
}
fn private(path: &Path, directory: bool) -> StorageResult<()> {
    let m = fs::symlink_metadata(path).map_err(io_error)?;
    if m.file_type().is_symlink() || if directory { !m.is_dir() } else { !m.is_file() } {
        return Err(io_error("archive path type differs"));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        if m.uid() != rustix::process::getuid().as_raw()
            || m.permissions().mode() & 0o077 != 0
            || (!directory && m.nlink() != 1)
        {
            return Err(io_error("archive path is not exclusively private"));
        }
    }
    Ok(())
}
fn create_private_directory(path: &Path) -> StorageResult<()> {
    if exists(path)? {
        return private(path, true);
    }
    fs::create_dir(path).map_err(io_error)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700)).map_err(io_error)?;
    }
    sync_directory(path)?;
    sync_directory(
        path.parent()
            .ok_or_else(|| io_error("archive parent missing"))?,
    )
}
impl ArchiveRegistration {
    fn validate(&self, journal: &ErasureJournal) -> StorageResult<()> {
        if !uuid(&self.lineage_id)
            || !uuid(&self.generation_id)
            || self
                .parent_generation_id
                .as_ref()
                .is_some_and(|v| !uuid(v) || v == &self.generation_id)
            || !hash(&self.binding_sha256)
            || !hash(&self.snapshot_sha256)
            || self.journal_id != journal.identity()
            || self.source_space_id.is_empty()
            || self.target_space_id.is_empty()
        {
            return Err(io_error("archive registration identity differs"));
        }
        Ok(())
    }
}
impl ArchiveSealEvidence {
    fn validate(&self) -> StorageResult<()> {
        let mut digest = Sha256::new();
        digest.update(b"recall-operational-archive-seal-v1\0");
        digest.update(serde_json::to_vec(&self.seal).map_err(io_error)?);
        if self.seal_sha256 != hex::encode(digest.finalize())
            || self.seal.binding_sha256 != self.binding.digest()?
            || self.file_sha256 != self.seal.file_sha256
            || self.bytes != self.seal.bytes
            || !hash(&self.seal_sha256)
            || !hash(&self.file_sha256)
            || self.bytes == 0
        {
            return Err(io_error("archive seal evidence differs"));
        }
        Ok(())
    }
}
fn redaction_successor<'a>(
    registry: &'a Registry,
    original: &ArchiveRegistryEntry,
    proof: &ArchiveRedactionProof,
) -> StorageResult<&'a ArchiveRegistryEntry> {
    let next = registry
        .entries
        .get(&proof.replacement_generation_id)
        .ok_or_else(|| io_error("archive redaction successor missing"))?;
    let before = original
        .evidence()
        .ok_or_else(|| io_error("archive redaction predecessor unsealed"))?;
    let after = next
        .evidence()
        .ok_or_else(|| io_error("archive redaction successor unsealed"))?;
    let mut expected = before.binding.clone();
    expected.parent_generation_sha256 = Some(before.file_sha256.clone());
    if next.registration.parent_generation_id.as_deref()
        != Some(original.registration.generation_id.as_str())
        || next.registration.lineage_id != original.registration.lineage_id
        || next.native_receipt_sha256() != original.native_receipt_sha256()
        || next.native_receipt_sha256().is_none()
        || after.binding != expected
        || !after.seal.redacted
        || after.seal.ordered_inventory_sha256 != proof.preserved_inventory_sha256
        || !hash(&proof.removed_identity_sha256)
        || !hash(&proof.preserved_inventory_sha256)
    {
        return Err(io_error("archive redaction proof differs"));
    }
    Ok(next)
}
fn resolve_redactions<'a>(
    registry: &'a Registry,
    original: &'a ArchiveRegistryEntry,
    prepared_recovery: bool,
) -> StorageResult<&'a ArchiveRegistryEntry> {
    let mut current = original;
    let mut seen = BTreeSet::new();
    loop {
        if !seen.insert(&current.registration.generation_id) || seen.len() > MAX_GENERATIONS {
            return Err(io_error("archive redaction lineage cycle/bound"));
        }
        match &current.state {
            ArchiveGenerationState::Committed { .. } => return Ok(current),
            ArchiveGenerationState::Redacting { prepared: None, .. } if prepared_recovery => {
                return Ok(current)
            }
            ArchiveGenerationState::Redacted { .. } => (),
            ArchiveGenerationState::Redacting {
                prepared: Some(_), ..
            } if prepared_recovery => (),
            _ => return Err(io_error("archive redaction is incomplete")),
        }
        current = redaction_successor(
            registry,
            current,
            &current
                .redaction_proof()
                .ok_or_else(|| io_error("archive redaction proof absent"))?,
        )?;
    }
}
fn validate_archive_registry_heads(r: &Registry) -> StorageResult<()> {
    for (lineage, id) in &r.heads {
        let e = r
            .entries
            .get(id)
            .ok_or_else(|| io_error("archive lineage head missing"))?;
        if &e.registration.lineage_id != lineage
            || !matches!(
                e.state,
                ArchiveGenerationState::Committed { .. }
                    | ArchiveGenerationState::Redacting { .. }
                    | ArchiveGenerationState::Redacted { .. }
            )
        {
            return Err(io_error("archive lineage head differs"));
        }
    }
    Ok(())
}

impl ErasureJournal {
    // Ancestors outside the managed root may be public (for example /tmp),
    // but no path component may redirect through a symlink. The journal and
    // every managed child are owned by this process user and private.
    fn validate_archive_ancestors(&self) -> StorageResult<()> {
        if !self.path().is_absolute() {
            return Err(io_error("archive journal path is not absolute"));
        }
        let mut path = PathBuf::new();
        for part in self.path().components() {
            match part {
                std::path::Component::RootDir | std::path::Component::Normal(_) => path.push(part),
                _ => return Err(io_error("archive journal path traversal")),
            }
            let metadata = fs::symlink_metadata(&path).map_err(io_error)?;
            if metadata.file_type().is_symlink() || !metadata.is_dir() {
                return Err(io_error("archive ancestor is not a directory"));
            }
        }
        private(self.path(), true)
    }
    fn verify_archive_file(
        &self,
        generation: &str,
        evidence: &ArchiveSealEvidence,
    ) -> StorageResult<()> {
        self.validate_archive_ancestors()?;
        let path = self.archive_payload_path(generation)?;
        private(&self.path().join(DIRECTORY), true)?;
        private(
            path.parent()
                .ok_or_else(|| io_error("archive parent missing"))?,
            true,
        )?;
        private(&path, false)?;
        for suffix in ["-wal", "-shm", "-journal"] {
            let mut sidecar = path.as_os_str().to_os_string();
            sidecar.push(suffix);
            if exists(Path::new(&sidecar))? {
                return Err(io_error("sealed archive has SQLite sidecar"));
            }
        }
        let mut file = fs::File::open(&path).map_err(io_error)?;
        let before = file.metadata().map_err(io_error)?;
        let mut hash = Sha256::new();
        let mut bytes = 0_u64;
        let mut buffer = vec![0_u8; 65_536].into_boxed_slice();
        loop {
            let n = file.read(&mut buffer).map_err(io_error)?;
            if n == 0 {
                break;
            }
            bytes = bytes
                .checked_add(n as u64)
                .ok_or_else(|| io_error("archive bytes overflow"))?;
            hash.update(&buffer[..n]);
        }
        let after = file.metadata().map_err(io_error)?;
        if bytes != evidence.bytes
            || before.len() != bytes
            || after.len() != bytes
            || before.modified().map_err(io_error)? != after.modified().map_err(io_error)?
            || hex::encode(hash.finalize()) != evidence.file_sha256
        {
            return Err(io_error("archive sealed file changed"));
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            let current = fs::symlink_metadata(&path).map_err(io_error)?;
            if before.dev() != current.dev()
                || before.ino() != current.ino()
                || before.ctime() != after.ctime()
                || before.ctime_nsec() != after.ctime_nsec()
            {
                return Err(io_error("archive file identity changed"));
            }
        }
        private(&path, false)
    }
    pub(crate) fn archive_payload_path(&self, generation: &str) -> StorageResult<PathBuf> {
        if !uuid(generation) {
            return Err(io_error("archive generation identity invalid"));
        }
        Ok(self
            .path()
            .join(DIRECTORY)
            .join(generation)
            .join("archive.sqlite"))
    }
    fn read_archive_registry(&self) -> StorageResult<Registry> {
        self.validate_archive_ancestors()?;
        let Some(selected) = self.operational_archive_registry_sha256()? else {
            return Ok(Registry {
                version: 1,
                journal_id: self.identity().into(),
                key_fingerprint: self.reference_key_fingerprint().into(),
                entries: BTreeMap::new(),
                heads: BTreeMap::new(),
                discarding_transfer: None,
            });
        };
        private(&self.path().join(REGISTRIES), true)?;
        let path = self
            .path()
            .join(REGISTRIES)
            .join(format!("{selected}.json"));
        private(&path, false)?;
        let mut bytes = Vec::new();
        fs::File::open(&path)
            .map_err(io_error)?
            .take(MAX_REGISTRY_BYTES + 1)
            .read_to_end(&mut bytes)
            .map_err(io_error)?;
        if bytes.len() as u64 > MAX_REGISTRY_BYTES {
            return Err(io_error("archive registry bound"));
        }
        let checked: CheckedRegistry =
            serde_json::from_slice(&bytes).map_err(|_| io_error("archive registry invalid"))?;
        if checked.checksum != selected || checked.checksum != digest(&checked.payload)? {
            return Err(io_error("archive registry checksum differs"));
        }
        let r = checked.payload;
        if r.version != 1
            || r.journal_id != self.identity()
            || r.key_fingerprint != self.reference_key_fingerprint()
            || r.entries.len() > MAX_GENERATIONS
        {
            return Err(io_error("archive registry identity/bound differs"));
        }
        let intents = self.intents()?;
        if let Some(snapshot) = &r.discarding_transfer {
            if self.transfer_marker()?.as_ref().map(|m| &m.source_sha256) != Some(snapshot)
                || !intents.is_empty()
                || r.entries.values().any(|e| {
                    e.registration.snapshot_sha256 != *snapshot
                        || e.registration.parent_generation_id.is_some()
                        || !matches!(
                            e.state,
                            ArchiveGenerationState::Pending
                                | ArchiveGenerationState::Sealed { .. }
                                | ArchiveGenerationState::Committed { .. }
                        )
                })
            {
                return Err(io_error("archive discard lacks exact original transfer"));
            }
        }
        self.validate_archive_registry_entries(&r, &intents)?;
        Ok(r)
    }
    fn validate_archive_registry_entries(
        &self,
        r: &Registry,
        intents: &[super::ErasureIntent],
    ) -> StorageResult<()> {
        for (key, e) in &r.entries {
            e.registration.validate(self)?;
            if key != &e.registration.generation_id {
                return Err(io_error("archive registry key differs"));
            }
            if let Some(parent) = &e.registration.parent_generation_id {
                let p = r
                    .entries
                    .get(parent)
                    .ok_or_else(|| io_error("archive predecessor missing"))?;
                if p.registration.lineage_id != e.registration.lineage_id
                    || p.registration.target_space_id != e.registration.target_space_id
                    || p.registration.source_space_id != e.registration.source_space_id
                {
                    return Err(io_error("archive predecessor binding differs"));
                }
            }
            let mut visited = BTreeSet::new();
            let mut parent = Some(key);
            while let Some(id) = parent {
                if !visited.insert(id) {
                    return Err(io_error("archive predecessor cycle"));
                }
                parent = r
                    .entries
                    .get(id)
                    .and_then(|v| v.registration.parent_generation_id.as_ref());
            }
            if let Some(evidence) = e.evidence() {
                let expected_parent = e
                    .registration
                    .parent_generation_id
                    .as_ref()
                    .and_then(|id| r.entries.get(id))
                    .and_then(ArchiveRegistryEntry::evidence)
                    .map(|parent| &parent.file_sha256);
                if evidence.binding.parent_generation_sha256.as_ref() != expected_parent
                    || evidence.binding.digest()? != e.registration.binding_sha256
                    || evidence.binding.snapshot_sha256 != e.registration.snapshot_sha256
                    || evidence.binding.journal_identity != e.registration.journal_id
                    || evidence.binding.source_space_id != e.registration.source_space_id
                    || evidence.binding.target_space_id != e.registration.target_space_id
                {
                    return Err(io_error("archive retained declaration differs"));
                }
            }
            match &e.state {
                ArchiveGenerationState::Pending => (),
                ArchiveGenerationState::Sealed { evidence } => evidence.validate()?,
                ArchiveGenerationState::Committed {
                    evidence,
                    native_receipt_sha256,
                } => {
                    evidence.validate()?;
                    if !hash(native_receipt_sha256) {
                        return Err(io_error("archive native receipt differs"));
                    }
                }
                ArchiveGenerationState::Redacting {
                    evidence,
                    native_receipt_sha256,
                    intent_id,
                    ..
                }
                | ArchiveGenerationState::Redacted {
                    evidence,
                    native_receipt_sha256,
                    intent_id,
                    ..
                } => {
                    evidence.validate()?;
                    if !hash(native_receipt_sha256)
                        || !intents.iter().any(|i| i.intent_id == *intent_id)
                    {
                        return Err(io_error("archive redaction has no original intent"));
                    }
                    if let Some(proof) = e.redaction_proof() {
                        redaction_successor(r, e, &proof)?;
                    }
                }
            }
        }
        validate_archive_registry_heads(r)
    }
    fn write_archive_registry(&self, r: Registry) -> StorageResult<String> {
        let checksum = digest(&r)?;
        let bytes = serde_json::to_vec(&CheckedRegistry {
            payload: r,
            checksum: checksum.clone(),
        })
        .map_err(io_error)?;
        if bytes.len() as u64 > MAX_REGISTRY_BYTES {
            return Err(io_error("archive registry bound"));
        }
        self.validate_archive_ancestors()?;
        let directory = self.path().join(REGISTRIES);
        create_private_directory(&directory)?;
        let path = directory.join(format!("{checksum}.json"));
        if exists(&path)? {
            private(&path, false)?;
            if fs::read(&path).map_err(io_error)? != bytes {
                return Err(io_error("immutable archive registry differs"));
            }
        } else {
            atomic_write(&path, &bytes)?;
        }
        Ok(checksum)
    }
    pub(super) fn validate_operational_archives(&self) -> StorageResult<()> {
        let r = self.read_archive_registry()?;
        if r.discarding_transfer.is_some() {
            // The independent original transfer marker keeps the database
            // unready while exact registered payload removals are resumed.
            return Ok(());
        }
        for e in r.entries.values() {
            let path = self.archive_payload_path(&e.registration.generation_id)?;
            if matches!(e.state, ArchiveGenerationState::Pending) {
                continue;
            }
            if matches!(e.state, ArchiveGenerationState::Redacted { .. }) {
                if exists(&path)? {
                    return Err(io_error("physically redacted archive payload remains"));
                }
                continue;
            }
            if matches!(
                &e.state,
                ArchiveGenerationState::Redacting {
                    prepared: Some(_),
                    ..
                }
            ) && !exists(&path)?
            {
                // A crash may remove the original after the exact successor was
                // prepared. Only that complete, hash-verified successor makes
                // this state replayable; arbitrary missing originals fail.
                let successor = resolve_redactions(&r, e, true)?;
                self.verify_archive_file(
                    &successor.registration.generation_id,
                    successor
                        .evidence()
                        .ok_or_else(|| io_error("redaction successor unsealed"))?,
                )?;
                continue;
            }
            private(
                path.parent()
                    .ok_or_else(|| io_error("archive generation parent missing"))?,
                true,
            )?;
            private(&path, false)?;
        }
        self.verify_current_archive_files()
    }
    pub(crate) fn verify_current_archive_files(&self) -> StorageResult<()> {
        let registry = self.read_archive_registry()?;
        if registry.discarding_transfer.is_some() {
            return Err(io_error("archive discard is pending"));
        }
        for id in registry.heads.values() {
            let original = registry
                .entries
                .get(id)
                .ok_or_else(|| io_error("archive head absent"))?;
            let current = if matches!(
                original.state,
                ArchiveGenerationState::Redacting { prepared: None, .. }
            ) {
                // Preserve the recovery handle: the durable intent will replay
                // before Ready. The still-retained original must be intact.
                original
            } else {
                resolve_redactions(&registry, original, true)?
            };
            self.verify_archive_file(
                &current.registration.generation_id,
                current
                    .evidence()
                    .ok_or_else(|| io_error("archive current head unsealed"))?,
            )?;
        }
        Ok(())
    }
    pub(crate) fn reserve_operational_archive(
        &self,
        registration: ArchiveRegistration,
    ) -> StorageResult<PathBuf> {
        registration.validate(self)?;
        let generation_id = registration.generation_id.clone();
        self.with_operational_archive_writer(|| {
            let mut r = self.read_archive_registry()?;
            if let Some(existing) = r.entries.get(&registration.generation_id) {
                if existing.registration != registration {
                    return Err(io_error("archive generation binding changed"));
                }
                if !matches!(existing.state, ArchiveGenerationState::Pending) {
                    return Err(io_error("archive candidate already sealed"));
                }
                return Ok((
                    self.archive_payload_path(&registration.generation_id)?,
                    digest(&r)?,
                ));
            }
            if r.entries.len() >= MAX_GENERATIONS {
                return Err(io_error("archive generation count bound"));
            }
            if registration.parent_generation_id.as_ref() != r.heads.get(&registration.lineage_id) {
                return Err(io_error("archive original head changed"));
            }
            if let Some(parent) = &registration.parent_generation_id {
                let e = r
                    .entries
                    .get(parent)
                    .ok_or_else(|| io_error("archive original head missing"))?;
                if !matches!(
                    e.state,
                    ArchiveGenerationState::Committed { .. }
                        | ArchiveGenerationState::Redacting { .. }
                ) {
                    return Err(io_error("archive predecessor not committed"));
                }
            }
            r.entries.insert(
                registration.generation_id.clone(),
                ArchiveRegistryEntry {
                    registration,
                    state: ArchiveGenerationState::Pending,
                },
            );
            let selected = self.write_archive_registry(r)?;
            Ok((self.archive_payload_path(&generation_id)?, selected))
        })?;
        create_private_directory(&self.path().join(DIRECTORY))?;
        let path = self.archive_payload_path(&generation_id)?;
        create_private_directory(
            path.parent()
                .ok_or_else(|| io_error("archive generation parent missing"))?,
        )?;
        Ok(path)
    }
    pub(crate) fn operational_archive_entry(
        &self,
        generation: &str,
    ) -> StorageResult<ArchiveRegistryEntry> {
        self.read_archive_registry()?
            .entries
            .remove(generation)
            .ok_or_else(|| io_error("archive generation not registered"))
    }
    pub(crate) fn seal_operational_archive(
        &self,
        original: &ArchiveRegistryEntry,
        evidence: ArchiveSealEvidence,
    ) -> StorageResult<ArchiveRegistryEntry> {
        evidence.validate()?;
        if evidence.binding.digest()? != original.registration.binding_sha256
            || evidence.binding.snapshot_sha256 != original.registration.snapshot_sha256
            || evidence.binding.journal_identity != original.registration.journal_id
            || evidence.binding.source_space_id != original.registration.source_space_id
            || evidence.binding.target_space_id != original.registration.target_space_id
        {
            return Err(io_error("archive seal registration differs"));
        }
        self.with_operational_archive_writer(|| {
            let mut r = self.read_archive_registry()?;
            let e = r
                .entries
                .get_mut(&original.registration.generation_id)
                .ok_or_else(|| io_error("archive generation missing"))?;
            if e.registration == original.registration
                && matches!(&e.state, ArchiveGenerationState::Sealed { evidence: saved } if saved == &evidence)
            {
                return Ok((e.clone(), digest(&r)?));
            }
            if e != original || !matches!(e.state, ArchiveGenerationState::Pending) {
                return Err(io_error("archive seal predecessor changed"));
            }
            self.verify_archive_file(&e.registration.generation_id, &evidence)?;
            e.state = ArchiveGenerationState::Sealed { evidence };
            let result = e.clone();
            let selected = self.write_archive_registry(r)?;
            Ok((result, selected))
        })
    }
    /// Snapshot the complete registered set; callers still need native and
    /// original-intent validation before exposing any original row bytes.
    pub(crate) fn operational_archive_entries(&self) -> StorageResult<Vec<ArchiveRegistryEntry>> {
        Ok(self
            .read_archive_registry()?
            .entries
            .into_values()
            .collect())
    }
    pub(crate) fn operational_archive_heads(&self) -> StorageResult<Vec<ArchiveRegistryEntry>> {
        let registry = self.read_archive_registry()?;
        registry
            .heads
            .values()
            .map(|id| {
                registry
                    .entries
                    .get(id)
                    .cloned()
                    .ok_or_else(|| io_error("archive head missing"))
            })
            .collect()
    }
    pub(crate) fn commit_operational_archive(
        &self,
        original: &ArchiveRegistryEntry,
        native_receipt_sha256: &str,
    ) -> StorageResult<ArchiveRegistryEntry> {
        if !hash(native_receipt_sha256) {
            return Err(io_error("archive native receipt digest invalid"));
        }
        self.with_operational_archive_writer(|| {
            let mut r = self.read_archive_registry()?;
            let e = r
                .entries
                .get(&original.registration.generation_id)
                .ok_or_else(|| io_error("archive commit generation missing"))?;
            let evidence = match &original.state {
                ArchiveGenerationState::Sealed { evidence } => evidence.clone(),
                _ => return Err(io_error("archive original is not sealed")),
            };
            let next = ArchiveRegistryEntry {
                registration: original.registration.clone(),
                state: ArchiveGenerationState::Committed {
                    evidence,
                    native_receipt_sha256: native_receipt_sha256.into(),
                },
            };
            if e == &next
                && r.heads.get(&e.registration.lineage_id) == Some(&e.registration.generation_id)
            {
                return Ok((e.clone(), digest(&r)?));
            }
            if e != original
                || r.heads.get(&e.registration.lineage_id)
                    != e.registration.parent_generation_id.as_ref()
            {
                return Err(io_error("archive commit predecessor differs"));
            }
            r.heads.insert(
                e.registration.lineage_id.clone(),
                e.registration.generation_id.clone(),
            );
            r.entries
                .insert(e.registration.generation_id.clone(), next.clone());
            let selected = self.write_archive_registry(r)?;
            Ok((next, selected))
        })
    }
    pub(crate) fn deny_operational_archive_for_intent(
        &self,
        original: &ArchiveRegistryEntry,
        intent: &super::ErasureIntent,
    ) -> StorageResult<ArchiveRegistryEntry> {
        if !self.intents()?.contains(intent) {
            return Err(io_error("archive denial lacks original erasure intent"));
        }
        self.with_operational_archive_writer(|| {
            let mut r = self.read_archive_registry()?;
            let current = r
                .entries
                .get(&original.registration.generation_id)
                .ok_or_else(|| io_error("archive erasure generation missing"))?;
            let (evidence, native_receipt_sha256) = match &original.state {
                ArchiveGenerationState::Committed {
                    evidence,
                    native_receipt_sha256,
                } => (evidence.clone(), native_receipt_sha256.clone()),
                _ => return Err(io_error("archive erasure original not committed")),
            };
            let next = ArchiveRegistryEntry {
                registration: original.registration.clone(),
                state: ArchiveGenerationState::Redacting {
                    evidence,
                    native_receipt_sha256,
                    intent_id: intent.intent_id.clone(),
                    prepared: None,
                },
            };
            if current == &next {
                return Ok((next, digest(&r)?));
            }
            if current != original {
                return Err(io_error("archive erasure predecessor changed"));
            }
            r.entries
                .insert(next.registration.generation_id.clone(), next.clone());
            let selected = self.write_archive_registry(r)?;
            Ok((next, selected))
        })
    }
    // Resolve only the exact current registry image. This is file authority,
    // not row authorization: every exposed row must separately pass the current
    // original-journal reference-membership check in its projected namespace.
    pub(crate) fn resolve_operational_archive_access(
        &self,
        entry: &ArchiveRegistryEntry,
    ) -> StorageResult<ArchiveRegistryEntry> {
        let registry = self.read_archive_registry()?;
        let actual = registry
            .entries
            .get(&entry.registration.generation_id)
            .ok_or_else(|| io_error("archive generation absent"))?;
        if actual != entry {
            return Err(io_error("archive generation changed"));
        }
        let resolved = resolve_redactions(&registry, actual, false)?;
        self.verify_archive_file(
            &resolved.registration.generation_id,
            resolved
                .evidence()
                .ok_or_else(|| io_error("archive current generation unsealed"))?,
        )?;
        Ok(resolved.clone())
    }
    // Explicit target discard owns the transfer marker and maintenance barrier.
    // Persist payload-removal intent before unlink; a crash remains repairable.
    pub(crate) fn discard_transfer_archive_payloads(&self, snapshot: &str) -> StorageResult<()> {
        if self
            .transfer_marker()?
            .as_ref()
            .map(|m| m.source_sha256.as_str())
            != Some(snapshot)
            || !self.intents()?.is_empty()
        {
            return Err(io_error("archive discard original transfer differs"));
        }
        let entries = self.with_operational_archive_writer(|| {
            let mut registry = self.read_archive_registry()?;
            if registry.entries.values().any(|e| {
                e.registration.snapshot_sha256 != snapshot
                    || e.registration.parent_generation_id.is_some()
                    || !matches!(
                        e.state,
                        ArchiveGenerationState::Pending
                            | ArchiveGenerationState::Sealed { .. }
                            | ArchiveGenerationState::Committed { .. }
                    )
            }) {
                return Err(io_error("archive discard contains unrelated generation"));
            }
            registry.discarding_transfer = Some(snapshot.into());
            let entries = registry.entries.values().cloned().collect::<Vec<_>>();
            let selected = self.write_archive_registry(registry)?;
            Ok((entries, selected))
        })?;
        for entry in entries {
            self.remove_registered_candidate_files(&entry)?;
        }
        Ok(())
    }
    pub(crate) fn finish_transfer_archive_discard(&self, snapshot: &str) -> StorageResult<()> {
        self.with_operational_archive_writer(|| {
            let mut registry = self.read_archive_registry()?;
            if registry.discarding_transfer.as_deref() != Some(snapshot) {
                return Err(io_error("archive discard completion differs"));
            }
            for entry in registry.entries.values() {
                let path = self.archive_payload_path(&entry.registration.generation_id)?;
                if exists(&path)? {
                    return Err(io_error("archive discarded payload remains"));
                }
            }
            registry.entries.clear();
            registry.heads.clear();
            registry.discarding_transfer = None;
            let selected = self.write_archive_registry(registry)?;
            Ok(((), selected))
        })
    }
    fn remove_registered_candidate_files(&self, entry: &ArchiveRegistryEntry) -> StorageResult<()> {
        self.validate_archive_ancestors()?;
        let path = self.archive_payload_path(&entry.registration.generation_id)?;
        let directory = path
            .parent()
            .ok_or_else(|| io_error("archive cleanup parent"))?;
        if !exists(directory)? {
            return Ok(());
        }
        private(&self.path().join(DIRECTORY), true)?;
        private(directory, true)?;
        for name in [
            "archive.sqlite",
            "archive.sqlite-wal",
            "archive.sqlite-shm",
            "archive.sqlite-journal",
        ] {
            let candidate = directory.join(name);
            if exists(&candidate)? {
                private(&candidate, false)?;
                fs::remove_file(candidate).map_err(io_error)?;
            }
        }
        // Required even on replay after unlink-before-fsync interruption.
        sync_directory(directory)
    }
    pub(crate) fn reset_pending_archive_transfer(
        &self,
        entry: &ArchiveRegistryEntry,
        authority: &crate::client::historical_catchup::PreparedHistoricalCatchupAuthority,
    ) -> StorageResult<()> {
        if authority.client()?.erasure_journal()?.identity() != self.identity() {
            return Err(io_error("archive partial cleanup journal differs"));
        }
        self.with_operational_archive_writer(|| {
            let registry = self.read_archive_registry()?;
            if registry.entries.get(&entry.registration.generation_id) != Some(entry)
                || !matches!(entry.state, ArchiveGenerationState::Pending)
                || registry.heads.get(&entry.registration.lineage_id)
                    != entry.registration.parent_generation_id.as_ref()
            {
                return Err(io_error("archive partial cleanup candidate differs"));
            }
            self.remove_registered_candidate_files(entry)?;
            Ok(((), digest(&registry)?))
        })
    }
    /// Discard only an incomplete, registered redaction attempt under its exact
    /// original erasure reservation. This never removes a sealed generation.
    pub(crate) fn reset_pending_archive_redaction(
        &self,
        entry: &ArchiveRegistryEntry,
        intent: &super::ErasureIntent,
    ) -> StorageResult<()> {
        if crate::client::current_erasure_reservation(self.identity()).as_deref()
            != Some(intent.intent_id.as_str())
            || !self.intents()?.contains(intent)
        {
            return Err(io_error("archive cleanup lacks erasure custody"));
        }
        self.with_operational_archive_writer(|| {
            let registry = self.read_archive_registry()?;
            if registry.entries.get(&entry.registration.generation_id) != Some(entry)
                || !matches!(entry.state, ArchiveGenerationState::Pending) {
                return Err(io_error("archive cleanup candidate changed"));
            }
            let parent = entry.registration.parent_generation_id.as_ref()
                .and_then(|id| registry.entries.get(id)).ok_or_else(||io_error("archive cleanup predecessor absent"))?;
            if !matches!(&parent.state, ArchiveGenerationState::Redacting { intent_id, prepared: None, .. } if intent_id == &intent.intent_id) {
                return Err(io_error("archive cleanup predecessor not reserved"));
            }
            self.validate_archive_ancestors()?;
            let path = self.archive_payload_path(&entry.registration.generation_id)?;
            let directory = path.parent().ok_or_else(|| io_error("archive cleanup parent"))?;
            private(&self.path().join(DIRECTORY), true)?;
            private(directory, true)?;
            // SQLite interruption can leave these known payload-bearing files.
            // No recursive directory deletion or unregistered path is accepted.
            for name in ["archive.sqlite", "archive.sqlite-wal", "archive.sqlite-shm", "archive.sqlite-journal"] {
                let candidate = directory.join(name);
                if exists(&candidate)? { private(&candidate, false)?; fs::remove_file(candidate).map_err(io_error)?; }
            }
            sync_directory(directory)?;
            Ok(((), digest(&registry)?))
        })
    }
    pub(crate) fn reserve_archive_redaction(
        &self,
        original: &ArchiveRegistryEntry,
        intent: &super::ErasureIntent,
        binding: crate::historical_operational_archive::ArchiveBinding,
    ) -> StorageResult<(ArchiveRegistryEntry, PathBuf)> {
        if !self.intents()?.contains(intent) {
            return Err(io_error("redaction original intent absent"));
        }
        let original_evidence = original
            .evidence()
            .ok_or_else(|| io_error("redaction original unsealed"))?;
        let mut expected_binding = original_evidence.binding.clone();
        expected_binding.parent_generation_sha256 = Some(original_evidence.file_sha256.clone());
        if binding != expected_binding {
            return Err(io_error("redaction declaration changed"));
        }
        let generation = uuid::Uuid::new_v5(
            &uuid::Uuid::parse_str(&original.registration.generation_id).map_err(io_error)?,
            intent.intent_id.as_bytes(),
        )
        .to_string();
        let registration = ArchiveRegistration {
            generation_id: generation.clone(),
            parent_generation_id: Some(original.registration.generation_id.clone()),
            binding_sha256: binding.digest()?,
            snapshot_sha256: binding.snapshot_sha256,
            journal_id: binding.journal_identity,
            source_space_id: binding.source_space_id,
            target_space_id: binding.target_space_id,
            ..original.registration.clone()
        };
        self.with_operational_archive_writer(|| {
            let mut registry=self.read_archive_registry()?;
            let current=registry.entries.get(&original.registration.generation_id).ok_or_else(||io_error("redaction original missing"))?;
            if current!=original || !matches!(&current.state,ArchiveGenerationState::Redacting {intent_id,..} if intent_id==&intent.intent_id) {
                return Err(io_error("redaction original changed"));
            }
            let entry=if let Some(existing)=registry.entries.get(&generation) {
                if existing.registration!=registration {return Err(io_error("redaction candidate changed"));}
                existing.clone()
            } else {
                if registry.entries.len()>=MAX_GENERATIONS {return Err(io_error("archive generation count bound"));}
                let entry=ArchiveRegistryEntry {registration,state:ArchiveGenerationState::Pending};
                registry.entries.insert(generation.clone(),entry.clone()); entry
            };
            let selected=self.write_archive_registry(registry)?;
            Ok((entry,selected))
        }).and_then(|entry| {
            create_private_directory(&self.path().join(DIRECTORY))?;
            let path=self.archive_payload_path(&generation)?;
            create_private_directory(path.parent().ok_or_else(||io_error("archive parent"))?)?;
            Ok((entry,path))
        })
    }
    pub(crate) fn prepare_archive_redaction(
        &self,
        original: &ArchiveRegistryEntry,
        successor: &ArchiveRegistryEntry,
        proof: ArchiveRedactionProof,
    ) -> StorageResult<ArchiveRegistryEntry> {
        self.with_operational_archive_writer(|| {
            let mut registry = self.read_archive_registry()?;
            let current = registry
                .entries
                .get(&original.registration.generation_id)
                .ok_or_else(|| io_error("redaction original missing"))?;
            let (evidence, receipt, intent_id) = match &original.state {
                ArchiveGenerationState::Redacting {
                    evidence,
                    native_receipt_sha256,
                    intent_id,
                    ..
                } => (
                    evidence.clone(),
                    native_receipt_sha256.clone(),
                    intent_id.clone(),
                ),
                _ => return Err(io_error("redaction not denied")),
            };
            if current != original
                || registry.entries.get(&successor.registration.generation_id) != Some(successor)
                || successor.registration.parent_generation_id.as_deref()
                    != Some(original.registration.generation_id.as_str())
                || successor.registration.lineage_id != original.registration.lineage_id
                || proof.replacement_generation_id != successor.registration.generation_id
                || !hash(&proof.removed_identity_sha256)
                || !hash(&proof.preserved_inventory_sha256)
            {
                return Err(io_error("redaction predecessor or proof changed"));
            }
            let next_evidence = match &successor.state {
                ArchiveGenerationState::Sealed { evidence } => evidence.clone(),
                _ => return Err(io_error("redaction successor not sealed")),
            };
            if !next_evidence.seal.redacted
                || next_evidence.seal.ordered_inventory_sha256 != proof.preserved_inventory_sha256
            {
                return Err(io_error("redaction survivor seal differs"));
            }
            let next = ArchiveRegistryEntry {
                registration: original.registration.clone(),
                state: ArchiveGenerationState::Redacting {
                    evidence,
                    native_receipt_sha256: receipt.clone(),
                    intent_id,
                    prepared: Some(proof),
                },
            };
            // Redaction successors carry the original native source-cut anchor;
            // they are not a new cut or a fabricated completion transaction.
            let sealed_successor = ArchiveRegistryEntry {
                registration: successor.registration.clone(),
                state: ArchiveGenerationState::Committed {
                    evidence: next_evidence,
                    native_receipt_sha256: receipt,
                },
            };
            registry.entries.insert(
                successor.registration.generation_id.clone(),
                sealed_successor,
            );
            registry
                .entries
                .insert(original.registration.generation_id.clone(), next.clone());
            let selected = self.write_archive_registry(registry)?;
            Ok((next, selected))
        })
    }
    pub(crate) fn finish_archive_redaction(
        &self,
        original: &ArchiveRegistryEntry,
    ) -> StorageResult<ArchiveRegistryEntry> {
        let ArchiveGenerationState::Redacting {
            prepared: Some(proof),
            ..
        } = &original.state
        else {
            return Err(io_error("redaction removal not prepared"));
        };
        let registry = self.read_archive_registry()?;
        if registry.entries.get(&original.registration.generation_id) != Some(original) {
            return Err(io_error("redaction prepared image changed"));
        }
        let next = redaction_successor(&registry, original, proof)?;
        let evidence = next
            .evidence()
            .ok_or_else(|| io_error("redaction survivor unsealed"))?;
        self.verify_archive_file(&next.registration.generation_id, evidence)?;
        self.validate_archive_ancestors()?;
        let path = self.archive_payload_path(&original.registration.generation_id)?;
        private(&self.path().join(DIRECTORY), true)?;
        private(
            path.parent().ok_or_else(|| io_error("archive parent"))?,
            true,
        )?;
        if exists(&path)? {
            self.verify_archive_file(
                &original.registration.generation_id,
                original
                    .evidence()
                    .ok_or_else(|| io_error("redaction original unsealed"))?,
            )?;
            fs::remove_file(&path).map_err(io_error)?;
        }
        // Also sync when replay observes absence: the previous owner may have
        // crashed after unlink but before the directory entry became durable.
        sync_directory(path.parent().ok_or_else(|| io_error("archive parent"))?)?;
        self.with_operational_archive_writer(|| {
            let mut registry = self.read_archive_registry()?;
            let current = registry
                .entries
                .get(&original.registration.generation_id)
                .ok_or_else(|| io_error("redaction original missing"))?;
            if current != original {
                return Err(io_error("redaction prepared image changed"));
            }
            let (evidence, native_receipt_sha256, intent_id) = match &original.state {
                ArchiveGenerationState::Redacting {
                    evidence,
                    native_receipt_sha256,
                    intent_id,
                    ..
                } => (
                    evidence.clone(),
                    native_receipt_sha256.clone(),
                    intent_id.clone(),
                ),
                _ => return Err(io_error("redaction original changed")),
            };
            let result = ArchiveRegistryEntry {
                registration: original.registration.clone(),
                state: ArchiveGenerationState::Redacted {
                    evidence,
                    native_receipt_sha256,
                    intent_id,
                    replacement_generation_id: proof.replacement_generation_id.clone(),
                    removed_identity_sha256: proof.removed_identity_sha256.clone(),
                    preserved_inventory_sha256: proof.preserved_inventory_sha256.clone(),
                },
            };
            registry
                .entries
                .insert(original.registration.generation_id.clone(), result.clone());
            let selected = self.write_archive_registry(registry)?;
            Ok((result, selected))
        })
    }
}

#[cfg(test)]
#[path = "erasure_operational_archives_tests.rs"]
mod tests;
