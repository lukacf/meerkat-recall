//! Strict authenticated request claims.
//!
//! This is an ingress contract, not a
//! grant evaluator, live revocation check, entry permit or resource operation.
//! Authenticated HTTP/MCP tool routes refuse this token type. No governed
//! endpoint is installed until resource and evidence owners exist.

use crate::{AuthError, ExactId, IssuerMappings, PrincipalClaim};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use jsonwebtoken::{decode, Algorithm, DecodingKey, Validation};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use std::{
    collections::BTreeSet,
    fmt,
    time::{SystemTime, UNIX_EPOCH},
};

/// Exact JWT type, refused by legacy ingress.
pub const EXCHANGE_TOKEN_TYPE: &str = "elephant-meerkat-request-v1+jwt";

/// Supported wire contract. Unknown versions fail deserialization.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExchangeVersion {
    /// First strict request exchange syntax.
    V1,
}

/// Requested profile. Its presence does not prove that it is available.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExchangeProfile {
    /// Buffered governed behavior requires the still-missing entry owners.
    GovernedBufferedV1,
}

/// Exact protocol on which the bound operation was observed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RequestTransport {
    /// HTTP method and route are carried in `operation_name`.
    Http,
    /// MCP tool identity is carried in `operation_name`.
    Mcp,
}

/// Lowercase SHA-256 over exact argument bytes.
///
/// No JSON reserialization or
/// normalization is implicit. Sender and trusted transport must use the same
/// exact byte encoding, and the observed binding must come from that transport.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct ArgumentDigest(String);

impl ArgumentDigest {
    /// Compute the V1 digest from actual transport argument bytes.
    #[must_use]
    pub fn of_bytes(bytes: &[u8]) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(b"elephant-exchange-exact-arguments-v1\0");
        hasher.update(bytes);
        Self(hex::encode(hasher.finalize()))
    }
}
impl TryFrom<String> for ArgumentDigest {
    type Error = AuthError;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        if value.len() != 64
            || !value
                .bytes()
                .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
        {
            return Err(AuthError::RequestMismatch);
        }
        Ok(Self(value))
    }
}
impl From<ArgumentDigest> for String {
    fn from(value: ArgumentDigest) -> Self {
        value.0
    }
}

/// Exact operation claim, compared with a separately constructed trusted
/// transport observation. It cannot be used as its own observation.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperationBinding {
    /// Stable operation identity for later canonical replay/entry ownership.
    pub operation_id: ExactId,
    /// Observed protocol.
    pub transport: RequestTransport,
    /// Exact method/route or tool identity from the feature-owned declaration.
    pub operation_name: ExactId,
    /// Elephant resource-authority identity, including its domain.
    pub resource_authority: PrincipalClaim,
    /// Exact space.
    pub space_id: ExactId,
    /// Exact feature-owned action, without wildcard interpretation.
    pub action: ExactId,
    /// Explicit bounded resource identities, never an inferred global selection.
    pub resources: BTreeSet<ExactId>,
    /// Exact processing destination.
    pub destination: ExactId,
    /// Intended recipient principals, not just a transport peer.
    pub audience: BTreeSet<PrincipalClaim>,
    /// Exact purpose set. Empty is explicit, not a default.
    pub purpose: BTreeSet<ExactId>,
    /// Actual argument-byte digest with the V1 domain separator.
    pub arguments_digest: ArgumentDigest,
}

/// A signed reference for later validation at its canonical grant authority.
/// This is not current delegation proof, and no epoch is synthesized here.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DelegationReference {
    /// Grant authority that must validate the reference.
    pub authority: PrincipalClaim,
    /// Exact authority-owned grant/lineage reference.
    pub reference: ExactId,
}

/// Issuer-attested observation, not proof of live revocation state.
///
/// The claimed interval starts at `checked_at` and ends exclusively at
/// `valid_until`. Verification requires the receiver's entire possible clock
/// interval to be inside it. The later independent-domain composition must
/// validate the authority and the permitted lease. Same-domain owners still
/// require their actual participating fence.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RevocationObservation {
    /// Canonical authority claimed by the attestation.
    pub authority: PrincipalClaim,
    /// Correlation reference to be resolved by that authority.
    pub reference: ExactId,
    /// Observation time in Unix seconds.
    pub checked_at: u64,
    /// End of the issuer's claimed interval, never silently extended.
    pub valid_until: u64,
}

/// Strict exchange token payload. All fields are required and unknown fields
/// refuse. These wire claims are safe to decode only as untrusted data.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExchangeClaims {
    /// Exact issuer selected by trusted verifier configuration.
    pub iss: ExactId,
    /// Exact Elephant service audience.
    pub aud: ExactId,
    /// Actor ID, also required to match actor.id.
    pub sub: ExactId,
    /// Issued at, in Unix seconds.
    pub iat: u64,
    /// Not before, in Unix seconds.
    pub nbf: u64,
    /// Expiry, in Unix seconds.
    pub exp: u64,
    /// Signed request identity, distinct from any future entry permission.
    pub jti: ExactId,
    /// Explicit version, without fallback.
    pub version: ExchangeVersion,
    /// Explicit requested profile, without fallback.
    pub profile: ExchangeProfile,
    /// Actor identity within the issuer's configured domain.
    pub actor: PrincipalClaim,
    /// Requester identity within the issuer's configured domain.
    pub requester: PrincipalClaim,
    /// Delegation references remain unverified until their owners participate.
    pub delegation: Vec<DelegationReference>,
    /// Exact bound operation.
    pub operation: OperationBinding,
    /// Required issuer observation, not a locally current grant.
    pub revocation: RevocationObservation,
}

redacted_debug!(
    ArgumentDigest,
    OperationBinding,
    DelegationReference,
    RevocationObservation,
    ExchangeClaims,
);

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ExchangeHeader {
    alg: Algorithm,
    typ: String,
    kid: ExactId,
}

/// Trusted verifier composition.
///
/// This initial implementation supports a single
/// explicitly selected HS256 key per verifier, not arbitrary issuer key lookup.
/// Identity mappings do not authorize Elephant attribute/clearance claims.
pub struct ExchangeVerifier<'a> {
    /// Secret installed out of band. Never read from an exchange token.
    pub secret: &'a str,
    /// Exact configured key identity.
    pub key_id: &'a ExactId,
    /// Exact configured issuer.
    pub issuer: &'a ExactId,
    /// Exact configured service audience.
    pub audience: &'a ExactId,
    /// Trusted identity domain mappings.
    pub mappings: &'a IssuerMappings,
    /// Explicit maximum token lifetime; zero refuses configuration.
    pub max_lifetime_seconds: u64,
    /// Trusted bound on receiver clock error, bounded by token lifetime.
    /// Acceptance shrinks by this amount at both ends, never extends expiry.
    pub max_clock_skew_seconds: u64,
    /// Explicit maximum observation interval; zero refuses configuration.
    pub max_observation_seconds: u64,
}

/// Authenticated and request-bound exchange claims.
///
/// No Deserialize, Serialize
/// or public constructor exists. This proves no replay acceptance, current
/// authority, resource entitlement or evidence durability. Cloning it never
/// creates another entered attempt. No route consumes it as a dispatch permit.
#[derive(Clone)]
pub struct AuthenticatedExchange {
    claims: ExchangeClaims,
}
impl fmt::Debug for AuthenticatedExchange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AuthenticatedExchange")
            .field("protected_metadata", &"redacted")
            .finish_non_exhaustive()
    }
}
impl AuthenticatedExchange {
    /// Protected claims for subsequent trusted owner validation only.
    #[must_use]
    pub const fn claims(&self) -> &ExchangeClaims {
        &self.claims
    }
}

impl ExchangeVerifier<'_> {
    /// Validate authentication, syntax, declared time bounds and exact binding.
    /// No clearance is imported and no operation is entered.
    ///
    /// # Errors
    /// Refuses mismatched/untrusted identities, key/header/claim ambiguity,
    /// invalid freshness and any difference from the trusted observed operation.
    pub fn verify(
        &self,
        token: &str,
        observed: &OperationBinding,
    ) -> Result<AuthenticatedExchange, AuthError> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| AuthError::InvalidFreshness)?
            .as_secs();
        self.verify_at(token, observed, now)
    }

    pub(crate) fn verify_at(
        &self,
        token: &str,
        observed: &OperationBinding,
        now: u64,
    ) -> Result<AuthenticatedExchange, AuthError> {
        if self.secret.trim().len() < 32
            || self.max_lifetime_seconds == 0
            || self.max_observation_seconds == 0
            || self.max_clock_skew_seconds > self.max_lifetime_seconds
        {
            return Err(AuthError::InvalidTrustConfiguration);
        }
        let header_bytes = URL_SAFE_NO_PAD
            .decode(token.split('.').next().ok_or(AuthError::InvalidToken)?)
            .map_err(|_| AuthError::InvalidToken)?;
        let header: ExchangeHeader =
            serde_json::from_slice(&header_bytes).map_err(|_| AuthError::InvalidToken)?;
        if header.alg != Algorithm::HS256
            || header.typ != EXCHANGE_TOKEN_TYPE
            || &header.kid != self.key_id
        {
            return Err(AuthError::UnsupportedProfile);
        }
        let mut validation = Validation::new(Algorithm::HS256);
        validation.set_required_spec_claims(&["exp", "nbf", "iss", "aud", "sub"]);
        validation.set_issuer(&[self.issuer.as_str()]);
        validation.set_audience(&[self.audience.as_str()]);
        // Use one clock below for all strict temporal claims. Signature,
        // algorithm, required claims, issuer and audience remain checked here.
        validation.validate_exp = false;
        validation.validate_nbf = false;
        let claims = decode::<ExchangeClaims>(
            token,
            &DecodingKey::from_secret(self.secret.as_bytes()),
            &validation,
        )?
        .claims;
        let mapping = self
            .mappings
            .resolve(claims.iss.as_str(), claims.aud.as_str())?;
        if claims.actor.domain() != &mapping.trust_domain
            || claims.requester.domain() != &mapping.trust_domain
            || claims.actor.id != claims.sub
        {
            return Err(AuthError::UnmappedIssuer);
        }
        if claims.actor != claims.requester && claims.delegation.is_empty() {
            return Err(AuthError::RequestMismatch);
        }
        if claims.operation != *observed
            || claims.operation.resources.is_empty()
            || claims.operation.audience.is_empty()
            || claims.jti != claims.operation.operation_id
        {
            return Err(AuthError::RequestMismatch);
        }
        let earliest = now
            .checked_sub(self.max_clock_skew_seconds)
            .ok_or(AuthError::InvalidFreshness)?;
        let latest = now
            .checked_add(self.max_clock_skew_seconds)
            .ok_or(AuthError::InvalidFreshness)?;
        let token_duration = claims
            .exp
            .checked_sub(claims.iat)
            .ok_or(AuthError::InvalidFreshness)?;
        let observation_duration = claims
            .revocation
            .valid_until
            .checked_sub(claims.revocation.checked_at)
            .ok_or(AuthError::InvalidFreshness)?;
        if claims.iat > earliest
            || claims.nbf < claims.iat
            || claims.nbf > earliest
            || claims.nbf >= claims.exp
            || latest >= claims.exp
            || token_duration == 0
            || token_duration > self.max_lifetime_seconds
            || claims.revocation.checked_at > earliest
            || latest >= claims.revocation.valid_until
            || observation_duration == 0
            || observation_duration > self.max_observation_seconds
            || claims.revocation.valid_until > claims.exp
        {
            return Err(AuthError::InvalidFreshness);
        }
        Ok(AuthenticatedExchange { claims })
    }
}
