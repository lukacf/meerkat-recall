//! Payload-free authenticated dependency on independently retained archives.
use super::{hash, invalid, verify_native_archive_heads, OperationalArchiveReceipt};
use crate::durability_io::{atomic_write, io_error};
use crate::erasure_journal::ErasureJournal;
use crate::snapshot::SnapshotManifest;
use crate::{StorageClient, StorageResult};
use serde::{Deserialize, Serialize};
use std::fs;
use std::io::Read;
use std::path::Path;

pub const SIDECAR: &str = "operational-archive-origin.json";
const MAX_BYTES: u64 = 16 * 1024 * 1024;
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Dependency {
    kind: String,
    journal_id: String,
    manifest_sha256: String,
    // Original native cuts, not transient journal redaction dispositions.
    heads: Vec<OperationalArchiveReceipt>,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Signed {
    dependency: Dependency,
    authentication: String,
}
fn heads(journal: &ErasureJournal) -> StorageResult<Vec<OperationalArchiveReceipt>> {
    journal
        .operational_archive_heads()?
        .into_iter()
        .map(|entry| {
            let evidence = entry
                .evidence()
                .ok_or_else(|| invalid("backup archive unsealed"))?;
            Ok(OperationalArchiveReceipt {
                registration: entry.registration.clone(),
                binding: evidence.binding.clone(),
                seal: evidence.seal.clone(),
                native_receipt_sha256: entry
                    .native_receipt_sha256()
                    .ok_or_else(|| invalid("backup archive uncommitted"))?
                    .into(),
            })
        })
        .collect()
}
fn authentication(journal: &ErasureJournal, value: &Dependency) -> StorageResult<String> {
    surrealdb::kvs::recall_history::backup_origin_journal_authenticator(
        journal.identity(),
        journal.reference_key_bytes(),
        &serde_json::to_vec(value).map_err(io_error)?,
    )
    .map_err(io_error)
}
pub async fn write(
    client: &StorageClient,
    output: &Path,
    manifest: &SnapshotManifest,
) -> StorageResult<()> {
    verify_native_archive_heads(client).await?;
    let journal = client.erasure_journal()?;
    let heads = heads(journal)?;
    if heads.is_empty() {
        return Ok(());
    }
    let dependency = Dependency {
        kind: "recall-operational-archive-backup-dependency-v1".into(),
        journal_id: journal.identity().into(),
        manifest_sha256: crate::snapshot::digest(&serde_json::to_vec(manifest).map_err(io_error)?),
        heads,
    };
    let signed = Signed {
        authentication: authentication(journal, &dependency)?,
        dependency,
    };
    let bytes = serde_json::to_vec(&signed).map_err(io_error)?;
    if bytes.len() as u64 > MAX_BYTES {
        return Err(invalid("archive backup dependency bound"));
    }
    atomic_write(&output.join(SIDECAR), &bytes)
}
pub fn preflight(
    journal: &ErasureJournal,
    input: &Path,
    manifest: &SnapshotManifest,
) -> StorageResult<()> {
    let expected = heads(journal)?;
    let path = input.join(SIDECAR);
    let metadata = match fs::symlink_metadata(&path) {
        Ok(metadata) => metadata,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound && expected.is_empty() => return Ok(()),
        Err(e) => return Err(io_error(e)),
    };
    let mut ancestor = std::path::PathBuf::new();
    for part in path.components() {
        if part == std::path::Component::ParentDir {
            return Err(invalid("archive backup path traversal"));
        }
        ancestor.push(part);
        if fs::symlink_metadata(&ancestor)
            .map_err(io_error)?
            .file_type()
            .is_symlink()
        {
            return Err(invalid("archive backup dependency path alias"));
        }
    }
    if !metadata.is_file() || metadata.len() > MAX_BYTES {
        return Err(invalid("archive backup dependency shape"));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        if metadata.uid() != rustix::process::getuid().as_raw()
            || metadata.nlink() != 1
            || metadata.permissions().mode() & 0o077 != 0
        {
            return Err(invalid("archive backup dependency custody"));
        }
    }
    let mut bytes = Vec::new();
    fs::File::open(&path)
        .map_err(io_error)?
        .take(MAX_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(io_error)?;
    if bytes.len() as u64 > MAX_BYTES {
        return Err(invalid("archive backup dependency length"));
    }
    let signed: Signed = serde_json::from_slice(&bytes).map_err(io_error)?;
    let mac = authentication(journal, &signed.dependency)?;
    let equal_mac = hash(&signed.authentication)
        && signed
            .authentication
            .bytes()
            .zip(mac.bytes())
            .fold(0_u8, |d, (a, b)| d | (a ^ b))
            == 0;
    if serde_json::to_vec(&signed).map_err(io_error)? != bytes
        || !equal_mac
        || signed.dependency.kind != "recall-operational-archive-backup-dependency-v1"
        || signed.dependency.journal_id != journal.identity()
        || signed.dependency.manifest_sha256
            != crate::snapshot::digest(&serde_json::to_vec(manifest).map_err(io_error)?)
        || signed.dependency.heads != expected
    {
        return Err(invalid(
            "backup requires its exact current original archive cut",
        ));
    }
    // Original cuts survive redaction, so a pre-erasure backup of the same cut
    // resolves through current independent redaction authority. A different cut
    // is rejected before replacement, leaving the live target untouched.
    journal.verify_current_archive_files()
}
