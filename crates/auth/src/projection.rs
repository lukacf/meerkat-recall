//! Existing HTTP/MCP policy projection, shared with durable admission checks.
//!
//! These pure transformations prove neither authentication nor authorization.
//! Intersect raw token and current grant scopes before projecting ingress.

use std::collections::HashSet;

use elephant_policy::Clearance;

/// Normalize the existing document, source and event scope aliases.
/// Wildcards remain wildcards; no explicit management scope is manufactured.
#[must_use]
pub fn operation_scopes(scopes: &[String]) -> HashSet<String> {
    let mut scopes: HashSet<String> = scopes.iter().cloned().collect();
    if scopes.contains("read:doc") {
        scopes.insert("read:doc:metadata".into());
    }
    if scopes.contains("read:doc:metadata") || scopes.contains("read:doc:raw") {
        scopes.insert("read:doc".into());
    }
    if scopes.contains("read:doc:raw") {
        scopes.insert("read:doc:metadata".into());
    }
    if scopes.contains("read:source:raw") {
        scopes.insert("read:source".into());
    }
    for (legacy, canonical) in [
        ("read:timeline", "read:event"),
        ("write:timeline", "write:event"),
    ] {
        if scopes.contains(legacy) || scopes.contains(canonical) {
            scopes.insert(legacy.into());
            scopes.insert(canonical.into());
        }
    }
    scopes
}

/// Preserve explicit ordered clearances, or project the existing two legacy
/// policies from signed flat fields when explicit clearances are absent.
#[must_use]
pub fn clearances(
    explicit: &[Clearance],
    security_level: i32,
    security_labels: &[String],
) -> Vec<Clearance> {
    if !explicit.is_empty() {
        return explicit.to_vec();
    }
    let labels: HashSet<String> = security_labels.iter().cloned().collect();
    vec![
        Clearance {
            policy_id: "default".into(),
            max_level: security_level,
            labels: labels.clone(),
            handling_allow: HashSet::new(),
        },
        Clearance {
            policy_id: "tenant_policy_v1".into(),
            max_level: security_level,
            labels,
            handling_allow: HashSet::from(["handling:trusted_context".into()]),
        },
    ]
}
