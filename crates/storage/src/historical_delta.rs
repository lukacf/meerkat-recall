//! Typed preparation for guarded historical catch-up. This module performs no
//! target writes and cannot construct historical maintenance authority.
use std::collections::BTreeSet;

use elephant_types::RecordId;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub use crate::historical_delta_vectors::{
    runtime_embedding_fingerprint, HistoricalEmbeddingDelta,
};
use crate::transfer_batch::TransferRecord;
use crate::{StorageError, StorageResult};

const MAX_BATCH_ROWS: usize = 256;
const MAX_BATCH_BYTES: usize = 256 * 1024 * 1024;
const MAX_ROW_BYTES: usize = 64 * 1024 * 1024;

/// A requested historical row transition; deletion never implies erase authority.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HistoricalChangeKind {
    /// Create an absent original record.
    Create,
    /// Replace content while preserving the declared lifecycle.
    Update,
    /// Set the successor soft-deletion marker with explicit destructive intent.
    SoftDelete,
    /// Restore a previously soft-deleted original record.
    Restore,
    /// Remove the target row while retaining snapshot evidence; this is not physical erasure.
    Remove,
}

/// Full typed before/after images, supplied by the verified snapshot adapter.
/// These fingerprints are runtime-domain hashes, not Python planner row hashes.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HistoricalDeltaRow {
    /// Original record identity shared by the before- and after-images.
    pub id: RecordId,
    /// Requested lifecycle or content transition.
    pub kind: HistoricalChangeKind,
    /// Complete original typed row, absent only for creation.
    pub before: Option<TransferRecord>,
    /// Complete successor typed row, absent only for removal.
    pub after: Option<TransferRecord>,
    /// Runtime-domain fingerprint of the before-image, or absent for creation.
    pub before_fingerprint: Option<String>,
    /// Runtime-domain fingerprint of the after-image, or absent for removal.
    pub after_fingerprint: Option<String>,
    /// Explicit caller intent for removal or soft deletion; never erasure authority.
    pub destructive_intent: bool,
}

/// Source-global maintenance evidence. This is retained as source provenance;
/// it never writes or grants authority over the target migration_history table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HistoricalRetainedControlDelta {
    /// Original source-global receipt, without target mutation authority.
    pub before: Option<serde_json::Value>,
    /// Successor source-global receipt, without target mutation authority.
    pub after: Option<serde_json::Value>,
    /// Runtime-domain fingerprint of the before-image, or absent for creation.
    pub before_fingerprint: Option<String>,
    /// Runtime-domain fingerprint of the after-image, or absent for removal.
    pub after_fingerprint: Option<String>,
}
/// Validate and hash the supported source-global purge receipt in its distinct runtime domain.
pub fn retained_control_fingerprint(row: &serde_json::Value) -> StorageResult<String> {
    let fields = row
        .as_object()
        .ok_or_else(|| refused("retained_control_shape"))?;
    if fields.len() != 4
        || !["id", "name", "checksum", "applied_at"]
            .iter()
            .all(|key| fields.contains_key(*key))
        || row["id"].as_str() != Some("migration_history:fake_embedding_purge_v1")
        || row["name"].as_str() != Some(crate::fake_embedding_repair::RECEIPT_NAME)
        || row["checksum"].as_str() != Some(crate::fake_embedding_repair::RECEIPT_CHECKSUM)
        || row["applied_at"]
            .as_str()
            .is_none_or(|value| chrono::DateTime::parse_from_rfc3339(value).is_err())
    {
        return Err(refused("retained_control_identity"));
    }
    let mut hash = Sha256::new();
    hash.update(b"recall-historical-retained-control/v1\0");
    hash.update(serde_json::to_vec(row).map_err(|_| refused("retained_control_serialization"))?);
    Ok(hex::encode(hash.finalize()))
}
impl HistoricalRetainedControlDelta {
    fn validate(&self) -> StorageResult<()> {
        if self.before.is_none() && self.after.is_none() || self.before == self.after {
            return Err(refused("retained_control_no_change"));
        }
        for (row, hash) in [
            (&self.before, &self.before_fingerprint),
            (&self.after, &self.after_fingerprint),
        ] {
            match (row, hash) {
                (None, None) => (),
                (Some(row), Some(hash)) if retained_control_fingerprint(row)? == *hash => (),
                _ => return Err(refused("retained_control_hash")),
            }
        }
        Ok(())
    }
}

/// Input declarations are not a capability to write an existing target.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HistoricalDeltaBatch {
    /// Version of this serialized declaration.
    pub version: u32,
    /// Digest identifying the complete immutable catch-up input.
    pub migration_id: String,
    /// Original snapshot namespace before the declared relocation.
    pub source_space_id: String,
    /// Destination namespace covered by this declaration.
    pub space_id: String,
    /// SHA-256 of the complete original S0 snapshot file.
    pub before_snapshot_sha256: String,
    /// SHA-256 of the complete successor S1 snapshot file.
    pub after_snapshot_sha256: String,
    /// SHA-256 of the independently bound offline delta plan.
    pub planner_delta_sha256: String,
    /// Zero-based position in the complete ordered batch sequence.
    pub batch_ordinal: u64,
    /// Preceding durable receipt digest, absent only for the first batch.
    pub previous_batch_receipt: Option<String>,
    /// Exact destination epoch required before this transition.
    pub expected_epoch: String,
    /// Ordinary typed row transitions in this atomic batch.
    pub rows: Vec<HistoricalDeltaRow>,
    #[serde(default)]
    /// Exact legacy vector transitions, without current profile approval.
    pub embeddings: Vec<HistoricalEmbeddingDelta>,
    #[serde(default)]
    /// Source-global evidence transitions retained without target control writes.
    pub retained_controls: Vec<HistoricalRetainedControlDelta>,
}

/// Validated input only. Source-owned authority must still prove journal,
/// adoption receipt, epoch, source state, policy and every target before-image.
#[derive(Debug)]
pub struct PreparedDeltaBatch {
    pub(crate) input: HistoricalDeltaBatch,
    pub(crate) digest: String,
}

fn refused(code: &str) -> StorageError {
    StorageError::DataIntegrityError(format!("historical_delta:{code}"))
}

fn valid_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

/// Exact runtime row image hash, including timestamps and the typed table tag.
///
/// The fixed domain prevents confusion with semantic support fingerprints,
/// which deliberately exclude `updated_at`, or HomeCore's raw JSON hashes.
pub fn runtime_row_fingerprint(row: &TransferRecord) -> StorageResult<String> {
    let value = serde_json::to_value(row).map_err(|_| refused("row_serialization"))?;
    if value.get("table").and_then(serde_json::Value::as_str)
        != Some(row.base().id.table().as_str())
    {
        return Err(refused("typed_table_identity"));
    }
    let bytes = serde_json::to_vec(row).map_err(|_| refused("row_serialization"))?;
    let mut hash = Sha256::new();
    hash.update(b"recall-historical-runtime-row/v1\0");
    hash.update(bytes);
    Ok(hex::encode(hash.finalize()))
}

impl HistoricalDeltaRow {
    fn validate(&self, space: &str) -> StorageResult<()> {
        if self.before.is_none() && self.after.is_none() {
            return Err(refused("empty_effect"));
        }
        for (row, fingerprint) in [
            (&self.before, &self.before_fingerprint),
            (&self.after, &self.after_fingerprint),
        ] {
            match (row, fingerprint) {
                (None, None) => (),
                (Some(row), Some(fingerprint))
                    if row.base().id == self.id
                        && row.base().space_id == space
                        && valid_digest(fingerprint)
                        && runtime_row_fingerprint(row)? == *fingerprint => {}
                _ => return Err(refused("row_identity_space_or_hash")),
            }
        }
        let expected_kind = match (&self.before, &self.after) {
            (None, Some(_)) => HistoricalChangeKind::Create,
            (Some(_), None) => HistoricalChangeKind::Remove,
            (Some(before), Some(after)) => {
                if self.before_fingerprint == self.after_fingerprint {
                    return Err(refused("unchanged_effect"));
                }
                match (
                    before.base().deleted_at.is_some(),
                    after.base().deleted_at.is_some(),
                ) {
                    (false, true) => HistoricalChangeKind::SoftDelete,
                    (true, false) => HistoricalChangeKind::Restore,
                    _ => HistoricalChangeKind::Update,
                }
            }
            (None, None) => return Err(refused("empty_effect")),
        };
        if self.kind != expected_kind
            || (matches!(
                self.kind,
                HistoricalChangeKind::SoftDelete | HistoricalChangeKind::Remove
            ) && !self.destructive_intent)
        {
            return Err(refused("kind_or_destructive_intent"));
        }
        Ok(())
    }

    /// Pure target-image check; the executor must perform it under its original
    /// transaction/custody fence, not use this result as a reusable permission.
    #[cfg(test)]
    pub(crate) fn validate_target(&self, current: Option<&TransferRecord>) -> StorageResult<()> {
        let actual = current.map(runtime_row_fingerprint).transpose()?;
        if actual != self.before_fingerprint {
            return Err(StorageError::Conflict(
                "historical_delta:before_image_changed".into(),
            ));
        }
        Ok(())
    }
}

impl HistoricalDeltaBatch {
    /// Validate bounded typed declarations without reading or changing a target.
    ///
    /// # Errors
    /// Refuses malformed identity, unbound ordering, duplicates and altered rows.
    pub fn prepare(self) -> StorageResult<PreparedDeltaBatch> {
        if self.version != 1
            || !valid_digest(&self.migration_id)
            || self.source_space_id.is_empty()
            || self.space_id.is_empty()
            || self.space_id.trim() != self.space_id
            || self.expected_epoch.is_empty()
            || ![
                &self.before_snapshot_sha256,
                &self.after_snapshot_sha256,
                &self.planner_delta_sha256,
            ]
            .iter()
            .all(|value| valid_digest(value))
            || (self.rows.is_empty()
                && self.embeddings.is_empty()
                && self.retained_controls.is_empty())
            || self
                .rows
                .len()
                .saturating_add(self.embeddings.len())
                .saturating_add(self.retained_controls.len())
                > MAX_BATCH_ROWS
            || self.retained_controls.len() > 1
            || (self.batch_ordinal == 0) != self.previous_batch_receipt.is_none()
            || self
                .previous_batch_receipt
                .as_ref()
                .is_some_and(|value| !valid_digest(value))
        {
            return Err(refused("batch_binding"));
        }
        for control in &self.retained_controls {
            control.validate()?;
        }
        let mut seen = BTreeSet::new();
        for row in &self.rows {
            if !seen.insert(&row.id) {
                return Err(refused("duplicate_identity"));
            }
            row.validate(&self.source_space_id)?;
            for original in [&row.before, &row.after].into_iter().flatten() {
                if serde_json::to_vec(original)
                    .map_err(|_| refused("row_serialization"))?
                    .len()
                    > MAX_ROW_BYTES
                {
                    return Err(refused("row_bytes"));
                }
            }
        }
        for row in &self.embeddings {
            if !seen.insert(&row.id) {
                return Err(refused("duplicate_identity"));
            }
            row.validate(&self.source_space_id)?;
        }
        let bytes = serde_json::to_vec(&self).map_err(|_| refused("batch_serialization"))?;
        if bytes.len() > MAX_BATCH_BYTES {
            return Err(refused("batch_bytes"));
        }
        let mut hash = Sha256::new();
        hash.update(b"recall-historical-runtime-batch/v1\0");
        hash.update(bytes);
        Ok(PreparedDeltaBatch {
            input: self,
            digest: hex::encode(hash.finalize()),
        })
    }
}

#[cfg(test)]
#[path = "historical_delta_tests.rs"]
mod tests;
