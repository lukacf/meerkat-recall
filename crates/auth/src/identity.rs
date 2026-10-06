//! Wire bindings for the canonical qualified principal contract.
//!
//! These claims authenticate no authority by themselves. JSON shape matches
//! Meerkat `PrincipalRef`; legacy unqualified identities are not accepted here.

use crate::AuthError;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Exact, nonempty identity without control characters. No normalization occurs.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct ExactId(String);

redacted_debug!(ExactId);

impl ExactId {
    /// Validate identity syntax, without asserting an issuer or authority.
    ///
    /// # Errors
    /// Refuses empty, whitespace-only and control-bearing identifiers.
    pub fn new(value: impl Into<String>) -> Result<Self, AuthError> {
        let value = value.into();
        if value.trim().is_empty() || value.chars().any(char::is_control) {
            return Err(AuthError::MalformedIdentity);
        }
        Ok(Self(value))
    }

    /// Exact identity text.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for ExactId {
    type Error = AuthError;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}
impl From<ExactId> for String {
    fn from(value: ExactId) -> Self {
        value.0
    }
}

/// Canonical Meerkat principal categories, with the same wire spellings.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PrincipalKind {
    /// Authenticated person.
    Human,
    /// Personal agent.
    PersonalAgent,
    /// Shared agent.
    SharedAgent,
    /// Runtime host.
    RuntimeHost,
    /// Service account.
    ServiceAccount,
}

/// Qualified-only subset of the canonical qualification wire contract.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum QualifiedDomain {
    /// Explicit authority domain, never inferred from the identifier.
    Qualified {
        /// Exact trusted domain identifier.
        trust_domain_id: ExactId,
    },
}

/// Unauthenticated wire claim matching a qualified canonical `PrincipalRef`.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PrincipalClaim {
    /// Canonical category.
    pub kind: PrincipalKind,
    /// Identity within its domain.
    pub id: ExactId,
    /// Explicit authority domain.
    pub qualification: QualifiedDomain,
}

redacted_debug!(QualifiedDomain, PrincipalClaim);

impl PrincipalClaim {
    /// Domain claimed by this wire value.
    #[must_use]
    pub const fn domain(&self) -> &ExactId {
        let QualifiedDomain::Qualified { trust_domain_id } = &self.qualification;
        trust_domain_id
    }
}

/// Trusted configuration mapping one exact issuer/audience pair to its domain.
/// This permits identity attestation, never arbitrary Elephant clearances.
#[derive(Debug, Clone)]
pub struct IssuerMapping {
    /// Exact verified issuer.
    pub issuer: ExactId,
    /// Exact verified audience.
    pub audience: ExactId,
    /// Domain whose identities this issuer may attest.
    pub trust_domain: ExactId,
    /// Explicit category for legacy tokens, which have no category claim.
    pub legacy_kind: PrincipalKind,
}

/// Validated trusted mappings. This object is not deserializable from a request.
#[derive(Debug, Clone)]
pub struct IssuerMappings {
    mappings: BTreeMap<(ExactId, ExactId), IssuerMapping>,
}

impl IssuerMappings {
    /// Construct exact mappings without ambiguous pair assignments.
    ///
    /// # Errors
    /// Empty mappings and duplicate issuer/audience pairs refuse configuration.
    pub fn new(mappings: impl IntoIterator<Item = IssuerMapping>) -> Result<Self, AuthError> {
        let mut result = BTreeMap::new();
        for mapping in mappings {
            let key = (mapping.issuer.clone(), mapping.audience.clone());
            if result.insert(key, mapping).is_some() {
                return Err(AuthError::InvalidTrustConfiguration);
            }
        }
        if result.is_empty() {
            return Err(AuthError::InvalidTrustConfiguration);
        }
        Ok(Self { mappings: result })
    }

    pub(crate) fn resolve(
        &self,
        issuer: &str,
        audience: &str,
    ) -> Result<&IssuerMapping, AuthError> {
        self.mappings
            .get(&(ExactId::new(issuer)?, ExactId::new(audience)?))
            .ok_or(AuthError::UnmappedIssuer)
    }
}
