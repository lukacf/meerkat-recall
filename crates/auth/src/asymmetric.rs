//! Operator-managed RS256 public keys for the existing bearer-claims profile.
//!
//! Keys are an immutable startup snapshot. Token headers cannot introduce keys,
//! URLs or issuers. Rotation replaces the trusted snapshot through server restart.

use std::{collections::BTreeMap, io::Read, path::Path};

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use jsonwebtoken::{decode, Algorithm, DecodingKey, Validation};
use serde::Deserialize;

use crate::{verified_context, AuthError, Claims, ExactId, IssuerMappings, VerifiedCallerContext};

const MAX_KEY_FILE_BYTES: usize = 65_536;
const MAX_KEYS: usize = 16;

/// Validated, public verification material. No signing secret is held here.
#[derive(Clone)]
pub struct TrustedJwks {
    keys: BTreeMap<String, DecodingKey>,
}

redacted_debug!(TrustedJwks);

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct KeySet {
    keys: Vec<PublicRsaKey>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PublicRsaKey {
    kid: String,
    kty: String,
    alg: String,
    #[serde(rename = "use")]
    usage: Option<String>,
    key_ops: Option<Vec<String>>,
    n: String,
    e: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct BearerHeader {
    alg: Algorithm,
    kid: String,
    typ: Option<String>,
}

impl TrustedJwks {
    /// Load a bounded public-only JWKS file selected by the operator.
    ///
    /// # Errors
    /// Refuses unreadable/oversized files and invalid or ambiguous public keys.
    pub fn from_file(path: &Path) -> Result<Self, AuthError> {
        let file = std::fs::File::open(path).map_err(|_| AuthError::InvalidTrustConfiguration)?;
        if !file
            .metadata()
            .map_err(|_| AuthError::InvalidTrustConfiguration)?
            .is_file()
        {
            return Err(AuthError::InvalidTrustConfiguration);
        }
        let mut bytes = Vec::new();
        file.take((MAX_KEY_FILE_BYTES + 1) as u64)
            .read_to_end(&mut bytes)
            .map_err(|_| AuthError::InvalidTrustConfiguration)?;
        Self::from_json(&bytes)
    }

    /// Parse 1–16 uniquely named RSA public keys, each pinned to RS256.
    ///
    /// The supported JWKS fields are `kid`, `kty`, `alg`, `n`, `e`, and optional
    /// `use: sig` and `key_ops: [verify]`. Private/unknown fields are refused.
    ///
    /// # Errors
    /// Refuses weak, malformed, duplicate, private or non-verification keys.
    pub fn from_json(bytes: &[u8]) -> Result<Self, AuthError> {
        if bytes.len() > MAX_KEY_FILE_BYTES {
            return Err(AuthError::InvalidTrustConfiguration);
        }
        let set: KeySet =
            serde_json::from_slice(bytes).map_err(|_| AuthError::InvalidTrustConfiguration)?;
        if set.keys.is_empty() || set.keys.len() > MAX_KEYS {
            return Err(AuthError::InvalidTrustConfiguration);
        }
        let mut keys = BTreeMap::new();
        for key in set.keys {
            if !valid_key_id(&key.kid)
                || key.kty != "RSA"
                || key.alg != "RS256"
                || key.usage.as_deref().is_some_and(|usage| usage != "sig")
                || key.key_ops.as_ref().is_some_and(|ops| ops != &["verify"])
                || keys.contains_key(&key.kid)
            {
                return Err(AuthError::InvalidTrustConfiguration);
            }
            let modulus = URL_SAFE_NO_PAD
                .decode(&key.n)
                .map_err(|_| AuthError::InvalidTrustConfiguration)?;
            let exponent = URL_SAFE_NO_PAD
                .decode(&key.e)
                .map_err(|_| AuthError::InvalidTrustConfiguration)?;
            // RSA 2048–8192, minimally encoded unsigned modulus and odd exponent.
            let modulus_bits = modulus.len() * 8
                - modulus
                    .first()
                    .map_or(0, |byte| byte.leading_zeros() as usize);
            if !(256..=1024).contains(&modulus.len())
                || !(2048..=8192).contains(&modulus_bits)
                || modulus.first() == Some(&0)
                || modulus.last().is_none_or(|byte| byte & 1 == 0)
                || exponent.is_empty()
                || exponent.len() > 4
                || exponent[0] == 0
                || exponent.last().is_none_or(|byte| byte & 1 == 0)
                || exponent == [1]
            {
                return Err(AuthError::InvalidTrustConfiguration);
            }
            let decoding = DecodingKey::from_rsa_components(&key.n, &key.e)
                .map_err(|_| AuthError::InvalidTrustConfiguration)?;
            keys.insert(key.kid, decoding);
        }
        Ok(Self { keys })
    }

    /// Verify the existing bearer profile against a fixed issuer and audience.
    ///
    /// # Errors
    /// Rejects unknown keys, unsupported headers/profiles, invalid signatures,
    /// expired/not-yet-valid tokens and missing or mismatched identity bindings.
    pub fn verify(
        &self,
        token: &str,
        issuer: &str,
        audience: &str,
        mappings: Option<&IssuerMappings>,
    ) -> Result<VerifiedCallerContext, AuthError> {
        ExactId::new(issuer).map_err(|_| AuthError::InvalidTrustConfiguration)?;
        ExactId::new(audience).map_err(|_| AuthError::InvalidTrustConfiguration)?;
        if token.len() > MAX_KEY_FILE_BYTES {
            return Err(AuthError::InvalidToken);
        }
        let encoded = token.split('.').next().ok_or(AuthError::InvalidToken)?;
        let header_bytes = URL_SAFE_NO_PAD
            .decode(encoded)
            .map_err(|_| AuthError::InvalidToken)?;
        let header: BearerHeader =
            serde_json::from_slice(&header_bytes).map_err(|_| AuthError::UnsupportedProfile)?;
        if header.alg != Algorithm::RS256
            || header.typ.as_deref().is_some_and(|kind| kind != "JWT")
            || !valid_key_id(&header.kid)
        {
            return Err(AuthError::UnsupportedProfile);
        }
        let key = self.keys.get(&header.kid).ok_or(AuthError::InvalidToken)?;
        let mut validation = Validation::new(Algorithm::RS256);
        validation.set_issuer(&[issuer]);
        validation.set_audience(&[audience]);
        validation.set_required_spec_claims(&["exp", "iss", "aud", "sub"]);
        validation.validate_nbf = true;
        validation.leeway = 0;
        let claims = decode::<Claims>(token, key, &validation)?.claims;
        // jsonwebtoken ignores a malformed optional nbf. Refuse it rather than
        // interpreting a signed time restriction as absent.
        let payload = URL_SAFE_NO_PAD
            .decode(token.split('.').nth(1).ok_or(AuthError::InvalidToken)?)
            .map_err(|_| AuthError::InvalidToken)?;
        let fields: serde_json::Map<String, serde_json::Value> =
            serde_json::from_slice(&payload).map_err(|_| AuthError::InvalidToken)?;
        if fields.get("nbf").is_some_and(|value| !value.is_u64()) {
            return Err(AuthError::InvalidFreshness);
        }
        ExactId::new(claims.sub.clone())?;
        verified_context(token, claims, Some(issuer), Some(audience), mappings)
    }
}

fn valid_key_id(value: &str) -> bool {
    !value.is_empty() && value.len() <= 128 && value.bytes().all(|byte| byte.is_ascii_graphic())
}

#[cfg(test)]
mod tests;
