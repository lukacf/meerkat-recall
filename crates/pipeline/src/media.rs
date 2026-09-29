//! Shared media helpers for multimodal rendering and extraction.

#[cfg(feature = "meerkat-extraction")]
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
#[cfg(feature = "meerkat-extraction")]
use base64::Engine;
use std::process::Command;
#[cfg(feature = "meerkat-extraction")]
use std::str::FromStr;
use std::{env, fs, path::PathBuf};

#[cfg(feature = "meerkat-extraction")]
use elephant_storage::{MediaAssetRepository, StorageClient};
#[cfg(feature = "meerkat-extraction")]
use elephant_types::records::doc::{DocMediaAttachment, DocMediaKind, DocMediaLocation};
#[cfg(feature = "meerkat-extraction")]
use elephant_types::records::media_asset::managed_asset_path;
#[cfg(feature = "meerkat-extraction")]
use elephant_types::RecordId;
use uuid::Uuid;

use crate::error::PipelineError;

/// Loads attachment bytes and resolved duration.
#[cfg(feature = "meerkat-extraction")]
pub(crate) async fn load_media_bytes(
    storage: Option<&StorageClient>,
    space_id: &str,
    attachment: &DocMediaAttachment,
) -> Result<(Vec<u8>, u64), PipelineError> {
    let bytes = match &attachment.location {
        DocMediaLocation::Path { path } => tokio::fs::read(path).await.map_err(|e| {
            PipelineError::InvalidInput(format!("Failed to read media path {path}: {e}"))
        })?,
        DocMediaLocation::Uri { uri } => {
            return Err(PipelineError::InvalidInput(format!(
                "URI-backed media attachments are not supported by workers: {uri}"
            )));
        }
        DocMediaLocation::ManagedAsset { asset_id } => {
            let record_id = RecordId::from_str(asset_id).map_err(|e| {
                PipelineError::InvalidInput(format!(
                    "Invalid managed media asset id {asset_id}: {e}"
                ))
            })?;
            let storage = storage.ok_or_else(|| {
                PipelineError::InvalidInput(
                    "Managed media asset loading requires storage access".to_string(),
                )
            })?;
            // The only payload-bearing read: plain get() serves the metadata
            // view without payload_base64.
            let asset = MediaAssetRepository::new(storage.clone())
                .get_with_payload(space_id, &record_id)
                .await
                .map_err(PipelineError::StorageError)?
                .ok_or_else(|| {
                    PipelineError::InvalidInput(format!(
                        "Managed media asset {asset_id} was not found in space {space_id}"
                    ))
                })?;
            if let Some(payload_base64) = asset.payload_base64 {
                BASE64_STANDARD.decode(payload_base64).map_err(|e| {
                    PipelineError::InvalidInput(format!(
                        "Managed media asset {asset_id} contains invalid base64 payload: {e}"
                    ))
                })?
            } else {
                let path = managed_asset_path(&record_id);
                tokio::fs::read(&path).await.map_err(|e| {
                    PipelineError::InvalidInput(format!(
                        "Failed to read legacy managed media asset {asset_id} at {}: {e}",
                        path.display()
                    ))
                })?
            }
        }
    };

    let duration_ms = match attachment.kind {
        DocMediaKind::Image => 0,
        DocMediaKind::Video => attachment.duration_ms.unwrap_or_else(|| {
            let extension_hint = media_extension_hint(&attachment.media_type);
            match &attachment.location {
                DocMediaLocation::Path { path } => probe_video_duration_ms(path),
                DocMediaLocation::ManagedAsset { .. } => {
                    probe_video_duration_ms_from_bytes(&bytes, extension_hint)
                }
                DocMediaLocation::Uri { .. } => Ok(0),
            }
            .unwrap_or(0)
        }),
    };

    if attachment.kind == DocMediaKind::Video && duration_ms == 0 {
        return Err(PipelineError::InvalidInput(format!(
            "Could not determine duration for video attachment {}",
            attachment.attachment_id
        )));
    }

    Ok((bytes, duration_ms))
}

#[cfg(feature = "meerkat-extraction")]
fn media_extension_hint(media_type: &str) -> Option<&'static str> {
    match media_type {
        "video/mp4" => Some("mp4"),
        "video/quicktime" => Some("mov"),
        "video/x-m4v" => Some("m4v"),
        "image/jpeg" => Some("jpg"),
        "image/png" => Some("png"),
        "image/webp" => Some("webp"),
        _ => None,
    }
}

/// Probes the duration of a video attachment located on disk.
pub fn probe_video_duration_ms(path: &str) -> Result<u64, PipelineError> {
    if let Some(duration_ms) = run_ffprobe_duration_ms(path)? {
        return Ok(duration_ms);
    }
    if let Some(duration_ms) = run_mp4parse_duration_ms(path)? {
        return Ok(duration_ms);
    }
    if let Some(duration_ms) = run_mdls_duration_ms(path)? {
        return Ok(duration_ms);
    }

    Err(PipelineError::InvalidInput(format!(
        "Could not determine duration for video attachment at {path}; provide duration_ms metadata"
    )))
}

/// Probes the duration of a video attachment from an in-memory payload by
/// staging it to a temporary file first.
pub fn probe_video_duration_ms_from_bytes(
    bytes: &[u8],
    extension_hint: Option<&str>,
) -> Result<u64, PipelineError> {
    let extension = extension_hint
        .map(|value| value.trim_start_matches('.'))
        .filter(|value| !value.is_empty())
        .unwrap_or("mp4");
    let path: PathBuf = env::temp_dir().join(format!(
        "elephant-video-probe-{}.{}",
        Uuid::new_v4(),
        extension
    ));
    fs::write(&path, bytes).map_err(|e| {
        PipelineError::InvalidInput(format!(
            "Failed to stage temporary video attachment at {}: {e}",
            path.display()
        ))
    })?;
    let result = probe_video_duration_ms(&path.display().to_string());
    let _ = fs::remove_file(&path);
    result
}

fn run_ffprobe_duration_ms(path: &str) -> Result<Option<u64>, PipelineError> {
    let output = Command::new("ffprobe")
        .args([
            "-v",
            "error",
            "-show_entries",
            "format=duration",
            "-of",
            "default=noprint_wrappers=1:nokey=1",
            path,
        ])
        .output();

    let output = match output {
        Ok(output) => output,
        Err(_) => return Ok(None),
    };

    if !output.status.success() {
        return Ok(None);
    }

    let value = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if value.is_empty() {
        return Ok(None);
    }

    let secs = value.parse::<f64>().map_err(|e| {
        PipelineError::InvalidInput(format!("Invalid ffprobe duration for {path}: {e}"))
    })?;
    Ok(Some((secs * 1000.0).round() as u64))
}

fn run_mp4parse_duration_ms(path: &str) -> Result<Option<u64>, PipelineError> {
    let extension = std::path::Path::new(path)
        .extension()
        .and_then(|value| value.to_str())
        .unwrap_or_default()
        .to_ascii_lowercase();
    if !matches!(extension.as_str(), "mp4" | "m4v") {
        return Ok(None);
    }

    let mut file = std::fs::File::open(path).map_err(|e| {
        PipelineError::InvalidInput(format!("Failed to open video attachment at {path}: {e}"))
    })?;
    let context = match mp4parse::read_mp4(&mut file) {
        Ok(context) => context,
        Err(_) => return Ok(None),
    };

    for track in context.tracks {
        if let (Some(duration), Some(timescale)) = (track.duration, track.timescale) {
            if duration.0 == u64::MAX || timescale.0 == 0 {
                continue;
            }
            let millis = ((u128::from(duration.0) * 1000) / u128::from(timescale.0)) as u64;
            if millis > 0 {
                return Ok(Some(millis));
            }
        }
    }

    Ok(None)
}

fn run_mdls_duration_ms(path: &str) -> Result<Option<u64>, PipelineError> {
    let output = Command::new("mdls")
        .args(["-raw", "-name", "kMDItemDurationSeconds", path])
        .output();

    let output = match output {
        Ok(output) => output,
        Err(_) => return Ok(None),
    };

    if !output.status.success() {
        return Ok(None);
    }

    let value = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if value.is_empty() || value == "(null)" {
        return Ok(None);
    }

    let secs = value.parse::<f64>().map_err(|e| {
        PipelineError::InvalidInput(format!("Invalid mdls duration for {path}: {e}"))
    })?;
    Ok(Some((secs * 1000.0).round() as u64))
}

#[cfg(all(test, feature = "meerkat-extraction"))]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;
    use elephant_types::records::doc::{DocMediaAttachment, DocMediaKind, DocMediaLocation};

    #[tokio::test]
    async fn test_load_media_bytes_rejects_uri_backed_media() {
        let attachment = DocMediaAttachment {
            attachment_id: "attachment:1".to_string(),
            kind: DocMediaKind::Image,
            media_type: "image/png".to_string(),
            location: DocMediaLocation::Uri {
                uri: "https://example.com/image.png".to_string(),
            },
            sha256: None,
            size_bytes: None,
            duration_ms: None,
            label: None,
        };

        let err = load_media_bytes(None, "default", &attachment)
            .await
            .expect_err("uri-backed media should be rejected");
        let message = err.to_string();
        assert!(message.contains("URI-backed media attachments are not supported"));
    }
}
