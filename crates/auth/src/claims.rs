//! Legacy trusted-bearer claims. These values alone prove no authentication.

use elephant_policy::Clearance;
use elephant_types::RecordId;
use serde::{Deserialize, Serialize};

/// Claims extracted from JWT tokens.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Claims {
    /// Subject (user or service identifier).
    pub sub: String,
    /// Expiration timestamp.
    pub exp: i64,
    /// Issued at timestamp.
    #[serde(default)]
    pub iat: i64,
    /// Issuer.
    #[serde(default)]
    pub iss: Option<String>,
    /// Audience.
    #[serde(default)]
    pub aud: Option<Vec<String>>,
    /// Space ID the token grants access to.
    #[serde(default)]
    pub space_id: Option<String>,
    /// Scopes/permissions granted.
    #[serde(default)]
    pub scopes: Vec<String>,
    /// Security level the principal can access.
    #[serde(default)]
    pub security_level: i32,
    /// Security labels the principal can access.
    #[serde(default)]
    pub security_labels: Vec<String>,
    /// ABAC clearances for different policies.
    #[serde(default)]
    pub clearances: Vec<Clearance>,
    /// Allowlist of subject entities (if restricted).
    #[serde(default)]
    pub subject_allowlist: Option<Vec<RecordId>>,
    /// Purpose labels for access.
    #[serde(default)]
    pub purpose: Vec<String>,
}

impl Claims {
    /// Checks if the claims include a specific scope.
    #[must_use]
    #[allow(dead_code)]
    pub fn has_scope(&self, scope: &str) -> bool {
        self.scopes.iter().any(|s| s == scope || s == "*")
    }

    /// Checks if the claims include any of the specified scopes.
    #[must_use]
    #[allow(dead_code)]
    pub fn has_any_scope(&self, scopes: &[&str]) -> bool {
        scopes.iter().any(|s| self.has_scope(s))
    }

    /// Checks if the claims include all of the specified scopes.
    #[must_use]
    #[allow(dead_code)]
    pub fn has_all_scopes(&self, scopes: &[&str]) -> bool {
        scopes.iter().all(|s| self.has_scope(s))
    }

    /// Gets the principal identifier.
    #[must_use]
    #[allow(dead_code)]
    pub fn principal(&self) -> &str {
        &self.sub
    }
}
