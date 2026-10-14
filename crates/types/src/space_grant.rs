//! Current per-space service grants, distinct from signed token ceilings.

use std::collections::BTreeSet;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Durable local authorization for a service subject in one named space.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SpaceGrant {
    /// Exact named space.
    pub space_id: String,
    /// Exact authenticated service subject.
    pub subject: String,
    /// Opaque generation for administrative compare-and-swap.
    pub generation: String,
    /// False immediately revokes future dispatch.
    pub active: bool,
    /// Local operation ceiling; token scopes remain independently authoritative.
    pub scopes: BTreeSet<String>,
    /// Administrative actor that last changed the grant.
    pub updated_by: String,
    /// Time of the last committed change.
    pub updated_at: DateTime<Utc>,
}

/// Intersects a signed operation ceiling with a current local grant.
/// Wildcards never manufacture explicit `manage:` service administration scopes.
#[must_use]
pub fn intersect_scopes(token: &[String], grant: &BTreeSet<String>) -> Vec<String> {
    let mut result = BTreeSet::new();
    for scope in token {
        if scope == "*" {
            result.extend(
                grant
                    .iter()
                    .filter(|scope| !scope.starts_with("manage:"))
                    .cloned(),
            );
        } else if grant.contains(scope) || (grant.contains("*") && !scope.starts_with("manage:")) {
            result.insert(scope.clone());
        }
    }
    result.into_iter().collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn current_grant_only_attenuates_token_scopes() {
        let grant = BTreeSet::from([
            "read:source".into(),
            "write:source".into(),
            "manage:grant".into(),
        ]);
        assert_eq!(
            intersect_scopes(&["read:source".into()], &grant),
            vec!["read:source"]
        );
        assert_eq!(
            intersect_scopes(&["*".into()], &grant),
            vec!["read:source", "write:source"]
        );
        assert!(intersect_scopes(&["read:source".into()], &BTreeSet::new()).is_empty());
        // Explicit administration must appear in both independent ceilings.
        let wildcard = BTreeSet::from(["*".into()]);
        assert!(intersect_scopes(&["manage:grant".into()], &wildcard).is_empty());
        assert!(intersect_scopes(&["manage:provider".into()], &wildcard).is_empty());
        assert_eq!(
            intersect_scopes(&["read:source".into()], &wildcard),
            vec!["read:source"]
        );
        assert_eq!(
            intersect_scopes(&["manage:grant".into()], &grant),
            vec!["manage:grant"]
        );
    }
}
