//! Lossless v2 record compaction after original identity/reference/hash capture.
use base64::engine::general_purpose::STANDARD_NO_PAD;
use base64::Engine as _;

use super::{
    incomplete, BTreeMap, InspectedSnapshotBlob, LimitedWriter, NativeInspectionValue,
    SnapshotInspectionLimits, StorageResult,
};

const BLOB_THRESHOLD: usize = 1024;

#[derive(Default)]
pub(super) struct Compaction {
    blobs: BTreeMap<String, InspectedSnapshotBlob>,
    unique_bytes: usize,
    logical_bytes: usize,
}

impl Compaction {
    pub(super) fn record(
        &mut self,
        record: &mut NativeInspectionValue,
        limits: &SnapshotInspectionLimits,
        report_bytes: &mut usize,
    ) -> StorageResult<()> {
        self.value(record, limits, report_bytes, true)
    }

    pub(super) fn into_blobs(self) -> BTreeMap<String, InspectedSnapshotBlob> {
        self.blobs
    }

    fn charge_logical(
        &mut self,
        bytes: usize,
        limits: &SnapshotInspectionLimits,
    ) -> StorageResult<()> {
        if bytes > limits.string_bytes {
            return Err(incomplete("compact_string_limit"));
        }
        self.logical_bytes = self
            .logical_bytes
            .checked_add(bytes)
            .filter(|&total| total <= limits.payload_bytes)
            .ok_or_else(|| incomplete("compact_logical_bytes_limit"))?;
        Ok(())
    }

    fn blob(
        &mut self,
        bytes: &[u8],
        limits: &SnapshotInspectionLimits,
        report_bytes: &mut usize,
    ) -> StorageResult<String> {
        let key = crate::snapshot::digest(bytes);
        let encoded = InspectedSnapshotBlob {
            bytes: bytes.len() as u64,
            base64: STANDARD_NO_PAD.encode(bytes),
        };
        if let Some(original) = self.blobs.get(&key) {
            if original != &encoded {
                return Err(incomplete("compact_digest_collision"));
            }
            return Ok(key);
        }
        self.unique_bytes = self
            .unique_bytes
            .checked_add(bytes.len())
            .filter(|&total| total <= limits.payload_bytes)
            .ok_or_else(|| incomplete("compact_unique_bytes_limit"))?;
        // Charge each dictionary entry when introduced, before retaining it.
        // The complete final report still receives its exact serialized bound.
        let mut writer = LimitedWriter {
            remaining: limits.report_bytes.saturating_sub(*report_bytes),
            written: 0,
        };
        serde_json::to_writer(&mut writer, &BTreeMap::from([(&key, &encoded)]))
            .map_err(|_| incomplete("report_limit"))?;
        *report_bytes += writer.written;
        self.blobs.insert(key.clone(), encoded);
        Ok(key)
    }

    fn value(
        &mut self,
        value: &mut NativeInspectionValue,
        limits: &SnapshotInspectionLimits,
        report_bytes: &mut usize,
        replace: bool,
    ) -> StorageResult<()> {
        match value {
            NativeInspectionValue::String(text) => {
                self.charge_logical(text.len(), limits)?;
                if replace && text.len() >= BLOB_THRESHOLD {
                    *value = NativeInspectionValue::StringRef(self.blob(
                        text.as_bytes(),
                        limits,
                        report_bytes,
                    )?);
                }
            }
            NativeInspectionValue::Bytes(encoded) => {
                if encoded.len() % 2 != 0 {
                    return Err(incomplete("compact_bytes_encoding"));
                }
                self.charge_logical(encoded.len() / 2, limits)?;
                if replace && encoded.len() / 2 >= BLOB_THRESHOLD {
                    let bytes = hex::decode(encoded.as_bytes())
                        .map_err(|_| incomplete("compact_bytes_encoding"))?;
                    *value =
                        NativeInspectionValue::BytesRef(self.blob(&bytes, limits, report_bytes)?);
                }
            }
            NativeInspectionValue::Array(values) => {
                for value in values {
                    self.value(value, limits, report_bytes, replace)?;
                }
            }
            NativeInspectionValue::Map(values) => {
                for value in values.values_mut() {
                    self.value(value, limits, report_bytes, replace)?;
                }
            }
            NativeInspectionValue::Thing(identity) => {
                // Physical identity/reference grammar remains exactly v1, even
                // for composite IDs; its leaf bytes still count every time.
                self.value(&mut identity.id, limits, report_bytes, false)?;
            }
            NativeInspectionValue::StringRef(_) | NativeInspectionValue::BytesRef(_) => {
                return Err(incomplete("compact_input_reference"));
            }
            _ => {}
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "snapshot_inspection_compact_tests.rs"]
mod tests;
