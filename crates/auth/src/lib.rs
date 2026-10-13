//! One token verifier for Elephant HTTP and MCP ingress.
//!
//! Verified contexts retain authentication evidence, not authorization. Their
//! private construction and absent Deserialize prevent raw claims or public
//! ABAC projections from becoming verified context. Governed execution remains
//! unavailable until resource generations, live delegation, entry and mandatory
//! evidence owners are composed. No serialized permit is defined here.

macro_rules! redacted_debug {
    ($($kind:ty),+ $(,)?) => {$(
        impl std::fmt::Debug for $kind {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.debug_struct(stringify!($kind))
                    .field("protected_metadata", &"redacted")
                    .finish_non_exhaustive()
            }
        }
    )+};
}

pub mod bootstrap;
mod claims;
pub mod contract;
pub mod exchange;
mod identity;
pub mod information;
pub mod projection;
pub mod restrictions;
pub mod service_token;
pub mod standing_watch;

pub use claims::Claims;
pub use identity::{
    ExactId, IssuerMapping, IssuerMappings, PrincipalClaim, PrincipalKind, QualifiedDomain,
};

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use jsonwebtoken::{decode, decode_header, Algorithm, DecodingKey, Validation};
use std::fmt;

/// Deliberately non-sensitive authentication failure categories.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum AuthError {
    /// Signature or token structure failed validation.
    #[error("token validation failed")]
    InvalidToken,
    /// JWT expiry failed validation.
    #[error("token has expired")]
    ExpiredToken,
    /// Issuer did not match its configured verifier.
    #[error("token issuer is invalid")]
    InvalidIssuer,
    /// Audience did not match its configured verifier.
    #[error("token audience is invalid")]
    InvalidAudience,
    /// Unsupported or unsafe trusted configuration.
    #[error("invalid token verifier configuration")]
    InvalidTrustConfiguration,
    /// No exact trusted identity mapping exists.
    #[error("issuer identity mapping is unavailable")]
    UnmappedIssuer,
    /// Empty or control-bearing identity.
    #[error("identity is malformed")]
    MalformedIdentity,
    /// Wrong token type or attempt to use exchange claims as legacy authority.
    #[error("token profile is not supported on this surface")]
    UnsupportedProfile,
    /// Exact observed request differs from the signed exchange claim.
    #[error("exchange request binding does not match")]
    RequestMismatch,
    /// Temporal evidence is invalid under declared bounds.
    #[error("exchange freshness is invalid")]
    InvalidFreshness,
}

impl From<jsonwebtoken::errors::Error> for AuthError {
    fn from(error: jsonwebtoken::errors::Error) -> Self {
        use jsonwebtoken::errors::ErrorKind;
        match error.kind() {
            ErrorKind::ExpiredSignature => Self::ExpiredToken,
            ErrorKind::InvalidIssuer => Self::InvalidIssuer,
            ErrorKind::InvalidAudience => Self::InvalidAudience,
            _ => Self::InvalidToken,
        }
    }
}

/// Borrowed legacy verifier configuration. Existing trusted behavior is retained.
/// No network key discovery or token-selected key is supported.
pub struct LegacyVerifier<'a> {
    /// Existing HMAC verifier secret.
    pub secret: &'a str,
    /// Algorithm fixed by trusted configuration, never by a token.
    pub algorithm: Algorithm,
    /// Existing optional exact audience requirement.
    pub audience: Option<&'a str>,
    /// Existing optional exact issuer requirement.
    pub issuer: Option<&'a str>,
    /// Optional explicit identity mappings supplied by trusted composition.
    pub mappings: Option<&'a IssuerMappings>,
}

/// Signature-verified bearer context. It is not bound to an exact operation and
/// never proves current clearance, delegation, revocation or replay protection.
#[derive(Clone)]
pub struct VerifiedCallerContext {
    claims: Claims,
    mapped_identity: Option<PrincipalClaim>,
}

impl fmt::Debug for VerifiedCallerContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("VerifiedCallerContext")
            .field("protected_metadata", &"redacted")
            .finish_non_exhaustive()
    }
}

impl VerifiedCallerContext {
    /// Verified legacy claims for existing local policy projection only.
    #[must_use]
    pub const fn claims(&self) -> &Claims {
        &self.claims
    }

    /// Identity qualified by explicit trusted mapping, if one was configured.
    #[must_use]
    pub const fn mapped_identity(&self) -> Option<&PrincipalClaim> {
        self.mapped_identity.as_ref()
    }
}

impl LegacyVerifier<'_> {
    /// Verify once for either HTTP or MCP, preserving all authenticated claims.
    ///
    /// # Errors
    /// Refuses invalid signatures, configured issuer/audience mismatches, non-HMAC
    /// algorithms, exchange token types and unresolved configured mappings.
    pub fn verify(&self, token: &str) -> Result<VerifiedCallerContext, AuthError> {
        if !matches!(
            self.algorithm,
            Algorithm::HS256 | Algorithm::HS384 | Algorithm::HS512
        ) {
            return Err(AuthError::InvalidTrustConfiguration);
        }
        let header = decode_header(token)?;
        if header.typ.as_deref().is_some_and(|kind| kind != "JWT") {
            return Err(AuthError::UnsupportedProfile);
        }
        let mut validation = Validation::new(self.algorithm);
        if let Some(audience) = self.audience {
            validation.set_audience(&[audience]);
        }
        if let Some(issuer) = self.issuer {
            validation.set_issuer(&[issuer]);
        }
        let data = decode::<Claims>(
            token,
            &DecodingKey::from_secret(self.secret.as_bytes()),
            &validation,
        )?;
        if data.claims.bootstrap_space.is_some() {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .ok()
                .and_then(|duration| i64::try_from(duration.as_secs()).ok())
                .ok_or(AuthError::InvalidFreshness)?;
            bootstrap::validate(&data.claims, now)?;
        }
        // Preserve unrelated legacy extension claims, but never silently drop
        // the restrictions of a signed exchange hybrid. This inspection only
        // rejects reserved profile fields; authenticated identity still comes
        // solely from the signature-verified Claims above.
        let payload = URL_SAFE_NO_PAD
            .decode(token.split('.').nth(1).ok_or(AuthError::InvalidToken)?)
            .map_err(|_| AuthError::InvalidToken)?;
        let fields: serde_json::Map<String, serde_json::Value> =
            serde_json::from_slice(&payload).map_err(|_| AuthError::InvalidToken)?;
        if fields.contains_key("bootstrap_space") && data.claims.bootstrap_space.is_none() {
            return Err(AuthError::UnsupportedProfile);
        }
        if [
            "version",
            "profile",
            "actor",
            "requester",
            "delegation",
            "operation",
            "revocation",
        ]
        .iter()
        .any(|field| fields.contains_key(*field))
        {
            return Err(AuthError::UnsupportedProfile);
        }
        let mapped_identity = self
            .mappings
            .map(|mappings| {
                // A qualified mapping requires both independently configured exact
                // verifier requirements, not an audience chosen out of token data.
                let issuer = self.issuer.ok_or(AuthError::InvalidTrustConfiguration)?;
                let audience = self.audience.ok_or(AuthError::InvalidTrustConfiguration)?;
                if data.claims.iss.as_deref() != Some(issuer)
                    || data
                        .claims
                        .aud
                        .as_ref()
                        .is_none_or(|values| values.as_slice() != [audience])
                {
                    return Err(AuthError::UnmappedIssuer);
                }
                let mapping = mappings.resolve(issuer, audience)?;
                Ok(PrincipalClaim {
                    kind: mapping.legacy_kind,
                    id: ExactId::new(data.claims.sub.clone())?,
                    qualification: QualifiedDomain::Qualified {
                        trust_domain_id: mapping.trust_domain.clone(),
                    },
                })
            })
            .transpose()?;
        Ok(VerifiedCallerContext {
            claims: data.claims,
            mapped_identity,
        })
    }
}

#[cfg(test)]
mod tests;
