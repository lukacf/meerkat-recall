//! Verified local assets for approved `FastEmbed` profiles. This module never
//! downloads a model or treats a mutable upstream model name as a revision.

use std::collections::BTreeMap;
#[cfg(feature = "fastembed")]
use std::io::Read;
use std::path::PathBuf;
#[cfg(feature = "fastembed")]
use std::path::{Component, Path};

use elephant_types::provider_binding::ResolvedModelBinding;
use serde::{Deserialize, Serialize};
#[cfg(feature = "fastembed")]
use sha2::{Digest, Sha256};

use crate::{EmbeddingError, EmbeddingResult};

pub(crate) fn validate_binding_identity(binding: &ResolvedModelBinding) -> EmbeddingResult<()> {
    version_root(binding).map(|_| ())
}

#[cfg(feature = "fastembed")]
const MANIFEST_LIMIT: u64 = 1024 * 1024;
#[cfg(feature = "fastembed")]
const TOKENIZER_LIMIT: u64 = 32 * 1024 * 1024;
#[cfg(feature = "fastembed")]
const MODEL_LIMIT: u64 = 2 * 1024 * 1024 * 1024;
#[cfg(feature = "fastembed")]
const TOTAL_LIMIT: u64 = 4 * 1024 * 1024 * 1024;

/// One immutable file whose content is bound by the approved manifest digest.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FastEmbedAsset {
    /// Relative file path within the immutable version directory.
    pub path: String,
    /// Lowercase SHA256 of the exact bytes loaded into the model.
    pub sha256: String,
    /// Exact file length, checked before allocation and again after reading.
    pub bytes: u64,
}

/// The exact manifest bytes are hashed into `ModelProfile.model_version`.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FastEmbedAssetManifest {
    /// Must be `meerkat-recall-fastembed-assets/v1`.
    pub format: String,
    /// Existing supported `FastEmbed` model identity, including quantization suffix.
    pub model: String,
    /// Dimension identity of the approved profile.
    pub dimensions: usize,
    /// Explicit approved preprocessing identity, including opt-in `bge-en-retrieval-v1`.
    pub preprocessing: String,
    /// ONNX model bytes.
    pub onnx: FastEmbedAsset,
    /// Tokenizer vocabulary and rules.
    pub tokenizer: FastEmbedAsset,
    /// Model configuration used by the tokenizer.
    pub config: FastEmbedAsset,
    /// Special token map.
    pub special_tokens_map: FastEmbedAsset,
    /// Tokenizer configuration.
    pub tokenizer_config: FastEmbedAsset,
    /// ONNX external initializers by their model-declared filename.
    #[serde(default)]
    pub external_initializers: BTreeMap<String, FastEmbedAsset>,
}

#[cfg(feature = "fastembed")]
struct VerifiedAssets {
    manifest: FastEmbedAssetManifest,
    onnx: Vec<u8>,
    tokenizer: Vec<u8>,
    config: Vec<u8>,
    special_tokens_map: Vec<u8>,
    tokenizer_config: Vec<u8>,
    external_initializers: BTreeMap<String, Vec<u8>>,
}

fn blocked(reason: &str) -> EmbeddingError {
    EmbeddingError::ProviderError(format!("provider_assets: {reason}"))
}

fn is_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

#[cfg(feature = "fastembed")]
fn relative_path(value: &str) -> EmbeddingResult<&Path> {
    let path = Path::new(value);
    if value.is_empty()
        || value.contains('\\')
        || path
            .components()
            .any(|part| !matches!(part, Component::Normal(_)))
    {
        return Err(blocked("asset path must be a contained relative file"));
    }
    Ok(path)
}

fn version_root(binding: &ResolvedModelBinding) -> EmbeddingResult<(PathBuf, &str)> {
    if binding.connection.provider != "fastembed" || !binding.provenance.operation.is_embedding() {
        return Err(blocked("approved FastEmbed operation required"));
    }
    binding
        .profile
        .validate_preprocessing("fastembed")
        .map_err(|_| blocked("unsupported FastEmbed preprocessing for model/dimensions"))?;
    let digest = binding
        .profile
        .model_version
        .strip_prefix("sha256:")
        .filter(|value| is_digest(value))
        .ok_or_else(|| blocked("FastEmbed model_version must pin the asset manifest SHA256"))?;
    if binding.connection.credential_ref.is_some() {
        return Err(blocked("local FastEmbed assets do not accept credentials"));
    }
    let endpoint = reqwest::Url::parse(&binding.connection.endpoint)
        .map_err(|_| blocked("FastEmbed endpoint must be an absolute local file URL"))?;
    if endpoint.scheme() != "file"
        || endpoint
            .host_str()
            .is_some_and(|host| !host.is_empty() && host != "localhost")
        || !endpoint.username().is_empty()
        || endpoint.password().is_some()
        || endpoint.query().is_some()
        || endpoint.fragment().is_some()
    {
        return Err(blocked(
            "FastEmbed endpoint must be an absolute local file URL",
        ));
    }
    let root = endpoint
        .to_file_path()
        .map_err(|()| blocked("FastEmbed endpoint must be an absolute local file URL"))?;
    if !root.is_absolute() {
        return Err(blocked("FastEmbed asset root must be absolute"));
    }
    Ok((root, digest))
}

#[cfg(feature = "fastembed")]
fn version_directory(binding: &ResolvedModelBinding) -> EmbeddingResult<(PathBuf, &str)> {
    let (root, digest) = version_root(binding)?;
    let root = root
        .canonicalize()
        .map_err(|_| blocked("approved FastEmbed asset root is unavailable"))?;
    let directory = root
        .join(digest)
        .canonicalize()
        .map_err(|_| blocked("approved FastEmbed model version is unavailable"))?;
    if !directory.starts_with(&root) || !directory.is_dir() {
        return Err(blocked(
            "FastEmbed version directory escapes approved asset root",
        ));
    }
    Ok((directory, digest))
}

#[cfg(feature = "fastembed")]
fn open_contained(root: &Path, path: &str, limit: u64) -> EmbeddingResult<std::fs::File> {
    let file = root
        .join(relative_path(path)?)
        .canonicalize()
        .map_err(|_| blocked("approved FastEmbed asset is unavailable"))?;
    if !file.starts_with(root) {
        return Err(blocked(
            "FastEmbed asset escapes approved version directory",
        ));
    }
    let stream =
        std::fs::File::open(file).map_err(|_| blocked("FastEmbed asset cannot be read"))?;
    let metadata = stream
        .metadata()
        .map_err(|_| blocked("FastEmbed asset metadata unavailable"))?;
    if !metadata.is_file() || metadata.len() > limit {
        return Err(blocked("FastEmbed asset is not a bounded regular file"));
    }
    Ok(stream)
}

#[cfg(feature = "fastembed")]
fn read_contained(root: &Path, path: &str, limit: u64) -> EmbeddingResult<Vec<u8>> {
    let stream = open_contained(root, path, limit)?;
    let mut bytes = Vec::new();
    stream
        .take(limit + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| blocked("FastEmbed asset read failed"))?;
    if bytes.len() as u64 > limit {
        return Err(blocked("FastEmbed asset exceeded its size bound"));
    }
    Ok(bytes)
}

#[cfg(feature = "fastembed")]
fn read_asset(
    root: &Path,
    asset: &FastEmbedAsset,
    limit: u64,
    total: &mut u64,
    retain_bytes: bool,
) -> EmbeddingResult<Vec<u8>> {
    if asset.bytes == 0 || asset.bytes > limit || !is_digest(&asset.sha256) {
        return Err(blocked("invalid FastEmbed asset size or digest"));
    }
    *total = total
        .checked_add(asset.bytes)
        .filter(|bytes| *bytes <= TOTAL_LIMIT)
        .ok_or_else(|| blocked("FastEmbed model exceeds total asset limit"))?;
    let mut stream = open_contained(root, &asset.path, asset.bytes)?.take(asset.bytes + 1);
    let mut hasher = Sha256::new();
    let mut observed = 0_u64;
    let mut bytes = Vec::new();
    let mut chunk = vec![0_u8; 64 * 1024].into_boxed_slice();
    loop {
        let length = stream
            .read(&mut chunk)
            .map_err(|_| blocked("FastEmbed asset read failed"))?;
        if length == 0 {
            break;
        }
        observed += length as u64;
        hasher.update(&chunk[..length]);
        if retain_bytes {
            bytes.extend_from_slice(&chunk[..length]);
        }
    }
    if observed != asset.bytes || format!("{:x}", hasher.finalize()) != asset.sha256 {
        return Err(blocked(
            "FastEmbed asset checksum or size differs from approved version",
        ));
    }
    Ok(bytes)
}

#[cfg(feature = "fastembed")]
fn verify_assets(
    binding: &ResolvedModelBinding,
    retain_bytes: bool,
) -> EmbeddingResult<VerifiedAssets> {
    let (directory, digest) = version_directory(binding)?;
    let bytes = read_contained(&directory, "manifest.json", MANIFEST_LIMIT)?;
    if format!("{:x}", Sha256::digest(&bytes)) != digest {
        return Err(blocked(
            "FastEmbed manifest differs from approved model version",
        ));
    }
    let manifest: FastEmbedAssetManifest =
        serde_json::from_slice(&bytes).map_err(|_| blocked("invalid FastEmbed asset manifest"))?;
    if manifest.format != "meerkat-recall-fastembed-assets/v1"
        || manifest.model != binding.profile.model
        || Some(manifest.dimensions) != binding.profile.dimensions
        || manifest.preprocessing != binding.profile.preprocessing
        || manifest.external_initializers.len() > 16
    {
        return Err(blocked(
            "FastEmbed manifest does not match approved profile",
        ));
    }
    let mut total = 0;
    let onnx = read_asset(
        &directory,
        &manifest.onnx,
        MODEL_LIMIT,
        &mut total,
        retain_bytes,
    )?;
    let tokenizer = read_asset(
        &directory,
        &manifest.tokenizer,
        TOKENIZER_LIMIT,
        &mut total,
        retain_bytes,
    )?;
    let config = read_asset(
        &directory,
        &manifest.config,
        TOKENIZER_LIMIT,
        &mut total,
        true,
    )?;
    let special_tokens_map = read_asset(
        &directory,
        &manifest.special_tokens_map,
        TOKENIZER_LIMIT,
        &mut total,
        retain_bytes,
    )?;
    let tokenizer_config = read_asset(
        &directory,
        &manifest.tokenizer_config,
        TOKENIZER_LIMIT,
        &mut total,
        true,
    )?;
    // These small, digest-verified configuration files are also retained during
    // readiness validation. ONNX bytes remain streamed unless loading a model.
    verified_sequence_limit(&manifest.model, &config, &tokenizer_config)?;
    let mut external_initializers = BTreeMap::new();
    for (name, asset) in &manifest.external_initializers {
        relative_path(name)?;
        external_initializers.insert(
            name.clone(),
            read_asset(&directory, asset, MODEL_LIMIT, &mut total, retain_bytes)?,
        );
    }
    Ok(VerifiedAssets {
        manifest,
        onnx,
        tokenizer,
        config,
        special_tokens_map,
        tokenizer_config,
        external_initializers,
    })
}

/// Maximum encoded sequence, including special tokens, for these exact assets.
/// `FastEmbed` 5.17.2 defaults to 512. BERT counts positions from zero; `MPNet`'s
/// non-padding position IDs start at `pad_token_id` + 1. Do not round a fractional
/// tokenizer limit up, or cast the very large BGE sentinel before bounding it.
#[cfg(feature = "fastembed")]
fn verified_sequence_limit(
    model: &str,
    config: &[u8],
    tokenizer_config: &[u8],
) -> EmbeddingResult<usize> {
    crate::provider::fastembed_model_spec(model)?;
    let config: serde_json::Value = serde_json::from_slice(config)
        .map_err(|_| blocked("invalid pinned FastEmbed model configuration"))?;
    let tokenizer: serde_json::Value = serde_json::from_slice(tokenizer_config)
        .map_err(|_| blocked("invalid pinned FastEmbed tokenizer configuration"))?;
    let mpnet = model.trim().eq_ignore_ascii_case("all-mpnet-base-v2");
    let model_type = if mpnet { "mpnet" } else { "bert" };
    if config.get("model_type").and_then(serde_json::Value::as_str) != Some(model_type) {
        return Err(blocked(
            "pinned FastEmbed model architecture differs from approved model",
        ));
    }
    let positions = config
        .get("max_position_embeddings")
        .and_then(serde_json::Value::as_u64)
        .filter(|value| *value > 0)
        .ok_or_else(|| blocked("pinned FastEmbed model has no valid position limit"))?;
    let padding = config
        .get("pad_token_id")
        .and_then(serde_json::Value::as_u64)
        .filter(|value| u32::try_from(*value).is_ok())
        .ok_or_else(|| blocked("pinned FastEmbed model has no valid padding token"))?;
    let positions = if mpnet {
        positions
            .checked_sub(padding + 1)
            .filter(|value| *value > 0)
            .ok_or_else(|| blocked("pinned MPNet position limit excludes all input"))?
    } else {
        positions
    };
    let tokenizer_limit = tokenizer
        .get("model_max_length")
        .and_then(serde_json::Value::as_f64)
        .filter(|value| value.is_finite() && *value >= 1.0 && value.fract() == 0.0)
        .ok_or_else(|| blocked("pinned FastEmbed tokenizer has no valid sequence limit"))?;
    // Conversion is bounded by the runtime's 512-token supported default.
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let tokenizer_limit = tokenizer_limit.min(512.0) as usize;
    let position_limit = usize::try_from(positions.min(512))
        .map_err(|_| blocked("pinned FastEmbed position limit is unrepresentable"))?;
    Ok(tokenizer_limit.min(position_limit))
}

/// Check provisioned bytes against the approved profile without inference,
/// downloads or a credential lookup. A build without `FastEmbed` fails explicitly.
///
/// # Errors
/// Missing, altered, unpinned or unsupported assets prevent provider readiness.
pub async fn validate_fastembed_assets(binding: &ResolvedModelBinding) -> EmbeddingResult<()> {
    #[cfg(feature = "fastembed")]
    {
        let binding = binding.clone();
        tokio::task::spawn_blocking(move || {
            let assets = verify_assets(&binding, false)?;
            let spec = crate::provider::fastembed_model_spec(&assets.manifest.model)?;
            if spec.dims != assets.manifest.dimensions {
                return Err(blocked(
                    "FastEmbed model dimension does not match approved profile",
                ));
            }
            Ok(())
        })
        .await
        .map_err(|_| blocked("FastEmbed asset validation task failed"))?
    }
    #[cfg(not(feature = "fastembed"))]
    {
        let _ = binding;
        Err(blocked("this build lacks the approved FastEmbed runtime"))
    }
}

#[cfg(feature = "fastembed")]
pub(crate) fn load_provider(
    binding: &ResolvedModelBinding,
    capacity: tokio::sync::OwnedSemaphorePermit,
) -> EmbeddingResult<crate::FastEmbedEmbeddingProvider> {
    use fastembed::{
        InitOptionsUserDefined, TextEmbedding, TokenizerFiles, UserDefinedEmbeddingModel,
    };
    let assets = verify_assets(binding, true)?;
    let spec = crate::provider::fastembed_model_spec(&assets.manifest.model)?;
    if spec.dims != assets.manifest.dimensions {
        return Err(blocked(
            "FastEmbed model dimension does not match approved profile",
        ));
    }
    let max_length = verified_sequence_limit(
        &assets.manifest.model,
        &assets.config,
        &assets.tokenizer_config,
    )?;
    let mut model = UserDefinedEmbeddingModel::new(
        assets.onnx,
        TokenizerFiles {
            tokenizer_file: assets.tokenizer,
            config_file: assets.config,
            special_tokens_map_file: assets.special_tokens_map,
            tokenizer_config_file: assets.tokenizer_config,
        },
    );
    model.pooling = TextEmbedding::get_default_pooling_method(&spec.model);
    model.quantization = TextEmbedding::get_quantization_mode(&spec.model);
    model.output_key.clone_from(
        &TextEmbedding::get_model_info(&spec.model)
            .map_err(|_| blocked("FastEmbed model metadata unavailable"))?
            .output_key,
    );
    for (name, bytes) in assets.external_initializers {
        model = model.with_external_initializer(name, bytes);
    }
    let inner = TextEmbedding::try_new_from_user_defined(
        model,
        InitOptionsUserDefined::new()
            .with_intra_threads(4)
            .with_max_length(max_length),
    )
    .map_err(|_| blocked("approved FastEmbed asset bundle could not initialize"))?;
    crate::FastEmbedEmbeddingProvider::from_verified_model(
        binding.profile.model.clone(),
        spec.dims,
        inner,
        Some(capacity),
    )
    .with_preprocessing(&binding.profile.preprocessing)
}

#[cfg(all(test, feature = "fastembed"))]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;
    use elephant_types::provider_binding::{
        ModelBindingProvenance, ModelOperation, ModelProfile, ProviderConnection, SpaceConfigToken,
    };

    struct Fixture {
        root: PathBuf,
        binding: ResolvedModelBinding,
    }
    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.root);
        }
    }

    fn fixture() -> Fixture {
        fixture_with_preprocessing("text-v1")
    }

    fn fixture_with_preprocessing(preprocessing: &str) -> Fixture {
        let root = std::env::temp_dir().join(format!("recall-model-test-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&root).unwrap();
        let bytes = b"test asset bytes; never passed to inference";
        let asset = FastEmbedAsset {
            path: "fixture.bin".into(),
            sha256: format!("{:x}", Sha256::digest(bytes)),
            bytes: bytes.len() as u64,
        };
        let config_bytes =
            br#"{"model_type":"bert","max_position_embeddings":512,"pad_token_id":0}"#;
        let tokenizer_config_bytes = br#"{"model_max_length":512,"pad_token":"[PAD]"}"#;
        let config = FastEmbedAsset {
            path: "config.json".into(),
            sha256: format!("{:x}", Sha256::digest(config_bytes)),
            bytes: config_bytes.len() as u64,
        };
        let tokenizer_config = FastEmbedAsset {
            path: "tokenizer_config.json".into(),
            sha256: format!("{:x}", Sha256::digest(tokenizer_config_bytes)),
            bytes: tokenizer_config_bytes.len() as u64,
        };
        let manifest = FastEmbedAssetManifest {
            format: "meerkat-recall-fastembed-assets/v1".into(),
            model: "bge-small-en-v1.5".into(),
            dimensions: 384,
            preprocessing: preprocessing.into(),
            onnx: asset.clone(),
            tokenizer: asset.clone(),
            config,
            special_tokens_map: asset,
            tokenizer_config,
            external_initializers: BTreeMap::new(),
        };
        let manifest = serde_json::to_vec(&manifest).unwrap();
        let digest = format!("{:x}", Sha256::digest(&manifest));
        let directory = root.join(&digest);
        std::fs::create_dir(&directory).unwrap();
        std::fs::write(directory.join("manifest.json"), manifest).unwrap();
        std::fs::write(directory.join("fixture.bin"), bytes).unwrap();
        std::fs::write(directory.join("config.json"), config_bytes).unwrap();
        std::fs::write(
            directory.join("tokenizer_config.json"),
            tokenizer_config_bytes,
        )
        .unwrap();
        let binding = ResolvedModelBinding {
            provenance: ModelBindingProvenance {
                config: SpaceConfigToken {
                    space_id: "named".into(),
                    config_id: "space-config".into(),
                    generation: 1,
                    policy_id: "default".into(),
                },
                operation: ModelOperation::QueryEmbedding,
                binding_id: "query".into(),
                profile_id: "local".into(),
            },
            connection: ProviderConnection {
                id: "local-assets".into(),
                provider: "fastembed".into(),
                account_id: "local".into(),
                endpoint: reqwest::Url::from_directory_path(&root)
                    .unwrap()
                    .to_string(),
                credential_ref: None,
                credential_generation: 0,
                revoked: false,
            },
            profile: ModelProfile {
                id: "local".into(),
                connection_id: "local-assets".into(),
                model: "bge-small-en-v1.5".into(),
                model_version: format!("sha256:{digest}"),
                dimensions: Some(384),
                preprocessing: preprocessing.into(),
                summary: None,
            },
        };
        Fixture { root, binding }
    }

    #[test]
    fn preprocessing_is_bound_by_original_manifest_and_new_content_digest() {
        let original = fixture();
        let full = fixture_with_preprocessing("text-full-v2");
        assert_ne!(
            original.binding.profile.model_version,
            full.binding.profile.model_version
        );
        for fixture in [&original, &full] {
            assert!(verify_assets(&fixture.binding, false).is_ok());
            let mut relabeled = fixture.binding.clone();
            relabeled.profile.preprocessing = if relabeled.profile.preprocessing == "text-v1" {
                "text-full-v2"
            } else {
                "text-v1"
            }
            .into();
            assert!(
                verify_assets(&relabeled, false).is_err(),
                "never relabel existing bytes"
            );
        }
        let unsupported = fixture_with_preprocessing("unknown-preprocessor");
        assert!(verify_assets(&unsupported.binding, false).is_err());
    }

    #[test]
    fn bge_retrieval_requires_its_own_manifest_and_keeps_original_assets() {
        use elephant_types::provider_binding::BGE_EN_RETRIEVAL_V1;
        let original = fixture();
        let retrieval = fixture_with_preprocessing(BGE_EN_RETRIEVAL_V1);
        assert_ne!(
            original.binding.profile.model_version,
            retrieval.binding.profile.model_version
        );
        let original_assets = verify_assets(&original.binding, true).unwrap();
        let retrieval_assets = verify_assets(&retrieval.binding, true).unwrap();
        assert_eq!(original_assets.onnx, retrieval_assets.onnx);
        assert_eq!(original_assets.tokenizer, retrieval_assets.tokenizer);
        for (fixture, other_recipe) in [(&original, BGE_EN_RETRIEVAL_V1), (&retrieval, "text-v1")] {
            let mut relabeled = fixture.binding.clone();
            relabeled.profile.preprocessing = other_recipe.into();
            assert!(verify_assets(&relabeled, false).is_err());
        }
        for (model, dimensions) in [("all-minilm-l6-v2", 384), ("bge-small-en-v1.5", 768)] {
            let mut wrong = retrieval.binding.clone();
            wrong.profile.model = model.into();
            wrong.profile.dimensions = Some(dimensions);
            assert!(validate_binding_identity(&wrong).is_err());
        }
        assert!(verify_assets(&original.binding, false).is_ok());
    }

    #[test]
    fn verified_model_and_tokenizer_limits_preserve_special_token_capacity() {
        let bert = br#"{"model_type":"bert","max_position_embeddings":512,"pad_token_id":0}"#;
        let mpnet = br#"{"model_type":"mpnet","max_position_embeddings":514,"pad_token_id":1}"#;
        let huge = br#"{"model_max_length":1000000000000000019884624838656}"#;
        assert_eq!(
            verified_sequence_limit("bge-small-en-v1.5", bert, huge).unwrap(),
            512
        );
        assert_eq!(
            verified_sequence_limit("all-mpnet-base-v2", mpnet, huge).unwrap(),
            512
        );
        assert_eq!(
            verified_sequence_limit("all-minilm-l6-v2", bert, br#"{"model_max_length":256}"#)
                .unwrap(),
            256
        );
        let smaller = br#"{"model_type":"mpnet","max_position_embeddings":64,"pad_token_id":1}"#;
        assert_eq!(
            verified_sequence_limit("all-mpnet-base-v2", smaller, huge).unwrap(),
            62
        );
        assert!(verified_sequence_limit("all-mpnet-base-v2", bert, huge).is_err());
        for invalid in [
            b"{}".as_slice(),
            br#"{"model_max_length":0}"#,
            br#"{"model_max_length":-1}"#,
            br#"{"model_max_length":7.5}"#,
            br#"{"model_max_length":"512"}"#,
        ] {
            assert!(verified_sequence_limit("bge-small-en-v1.5", bert, invalid).is_err());
        }
        for invalid in [
            b"{}".as_slice(),
            br#"{"model_type":"bert","max_position_embeddings":0,"pad_token_id":0}"#,
            br#"{"model_type":"bert","max_position_embeddings":512}"#,
        ] {
            assert!(verified_sequence_limit("bge-small-en-v1.5", invalid, huge).is_err());
        }
        let no_capacity = br#"{"model_type":"mpnet","max_position_embeddings":2,"pad_token_id":1}"#;
        assert!(verified_sequence_limit("all-mpnet-base-v2", no_capacity, huge).is_err());
    }

    #[test]
    fn verified_manifest_binds_loaded_bytes_and_approved_profile() {
        let fixture = fixture();
        let assets = verify_assets(&fixture.binding, true).unwrap();
        assert_eq!(assets.onnx, b"test asset bytes; never passed to inference");
        let mut changed = fixture.binding.clone();
        changed.profile.model = "all-minilm-l6-v2".into();
        assert!(verify_assets(&changed, true).is_err());
        changed = fixture.binding.clone();
        changed.profile.dimensions = Some(1536);
        assert!(verify_assets(&changed, true).is_err());
        let (directory, _) = version_directory(&fixture.binding).unwrap();
        std::fs::write(directory.join("fixture.bin"), b"changed bytes").unwrap();
        assert!(verify_assets(&fixture.binding, true).is_err());
    }

    #[test]
    fn mutable_version_remote_endpoint_and_changed_manifest_are_refused() {
        let fixture = fixture();
        let mut changed = fixture.binding.clone();
        changed.profile.model_version = "main".into();
        assert!(verify_assets(&changed, true).is_err());
        changed = fixture.binding.clone();
        changed.connection.endpoint = "https://huggingface.co".into();
        assert!(verify_assets(&changed, true).is_err());
        let (directory, _) = version_directory(&fixture.binding).unwrap();
        std::fs::write(directory.join("manifest.json"), b"{}").unwrap();
        assert!(verify_assets(&fixture.binding, true).is_err());
    }

    #[test]
    fn asset_paths_and_size_bounds_fail_before_model_initialization() {
        let fixture = fixture();
        let (directory, _) = version_directory(&fixture.binding).unwrap();
        assert!(read_contained(&directory, "../outside", 100).is_err());
        assert!(read_contained(&directory, "/etc/passwd", 100).is_err());
        assert!(read_contained(&directory, "fixture.bin", 1).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn asset_symlinks_outside_pinned_version_are_refused() {
        let fixture = fixture();
        let (directory, _) = version_directory(&fixture.binding).unwrap();
        let target = fixture.root.join("outside");
        std::fs::write(&target, b"external bytes").unwrap();
        std::os::unix::fs::symlink(target, directory.join("escape")).unwrap();
        assert!(read_contained(&directory, "escape", 100).is_err());
    }
}
