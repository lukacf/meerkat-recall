//! Managed media asset record type.
//!
//! Inline media uploads are normalized into managed assets so documents can
//! reference typed media without embedding large payloads directly.

use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::ids::RecordId;

use super::base::BaseFields;
use super::doc::DocMediaKind;

/// Environment variable controlling the managed media asset root.
pub const MEDIA_ASSET_ROOT_ENV: &str = "PIPELINE_MULTIMODAL_ASSET_ROOT";
/// Environment variable controlling the maximum inline image payload size.
pub const INLINE_IMAGE_MAX_BYTES_ENV: &str = "PIPELINE_MULTIMODAL_INLINE_IMAGE_MAX_BYTES";
/// Environment variable controlling the maximum inline video payload size.
pub const INLINE_VIDEO_MAX_BYTES_ENV: &str = "PIPELINE_MULTIMODAL_INLINE_VIDEO_MAX_BYTES";

const DEFAULT_INLINE_IMAGE_MAX_BYTES: u64 = 64 * 1024 * 1024;
const DEFAULT_INLINE_VIDEO_MAX_BYTES: u64 = 512 * 1024 * 1024;

/// Maximum number of media attachments accepted per document on public
/// ingest surfaces. Bounds the size of a single render/extraction work item
/// (one model call carries every attachment).
pub const MAX_DOC_MEDIA_ATTACHMENTS: usize = 32;

/// Returns the managed media asset root directory.
#[must_use]
pub fn media_asset_root() -> PathBuf {
    std::env::var_os(MEDIA_ASSET_ROOT_ENV)
        .map_or_else(|| PathBuf::from(".elephant/media-assets"), PathBuf::from)
}

/// Returns the configured inline media size limit for the given kind.
#[must_use]
pub fn inline_media_max_bytes(kind: DocMediaKind) -> u64 {
    let (env_var, default) = match kind {
        DocMediaKind::Image => (INLINE_IMAGE_MAX_BYTES_ENV, DEFAULT_INLINE_IMAGE_MAX_BYTES),
        DocMediaKind::Video => (INLINE_VIDEO_MAX_BYTES_ENV, DEFAULT_INLINE_VIDEO_MAX_BYTES),
    };

    std::env::var(env_var)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(default)
}

/// Returns the deterministic relative storage path for a managed asset.
#[must_use]
pub fn managed_asset_relative_path(asset_id: &RecordId) -> String {
    format!("{}/{}", asset_id.table().as_str(), asset_id.id())
}

/// Returns the absolute filesystem path for a managed asset.
#[must_use]
pub fn managed_asset_path(asset_id: &RecordId) -> PathBuf {
    media_asset_root().join(managed_asset_relative_path(asset_id))
}

/// A managed media asset stored by Elephant.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MediaAsset {
    /// Base fields common to all records.
    #[serde(flatten)]
    pub base: BaseFields,

    /// Media kind.
    pub kind: DocMediaKind,

    /// MIME type.
    pub media_type: String,

    /// Storage path relative to the configured asset root.
    pub storage_path: String,

    /// Byte length of the stored payload.
    pub size_bytes: u64,

    /// SHA-256 fingerprint of the payload.
    pub sha256: String,

    /// Inline payload bytes encoded as base64 when Elephant manages the blob directly.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload_base64: Option<String>,

    /// Optional duration for video assets.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub duration_ms: Option<u64>,

    /// Optional source filename or label.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;
    use crate::ids::{RecordId, TableName};
    use std::str::FromStr;

    #[test]
    fn test_media_asset_roundtrip() {
        let asset = MediaAsset {
            base: BaseFields::new(
                RecordId::new_v4(TableName::MediaAsset),
                "space_1".to_string(),
                "test".to_string(),
            ),
            kind: DocMediaKind::Image,
            media_type: "image/png".to_string(),
            storage_path: "default/asset.png".to_string(),
            size_bytes: 321,
            sha256: "abc123".to_string(),
            payload_base64: Some("AQID".to_string()),
            duration_ms: None,
            label: Some("asset.png".to_string()),
        };

        let encoded = serde_json::to_string(&asset).expect("encode");
        let decoded: MediaAsset = serde_json::from_str(&encoded).expect("decode");
        assert_eq!(decoded, asset);
    }

    #[test]
    fn test_managed_asset_relative_path_is_deterministic() {
        let asset_id = RecordId::from_str("media_asset:550e8400-e29b-41d4-a716-446655440000")
            .expect("asset id");

        let path = managed_asset_relative_path(&asset_id);

        assert_eq!(path, "media_asset/550e8400-e29b-41d4-a716-446655440000");
    }
}
