//! Noninteractive local provisioning for the shared HTTP/MCP bearer contract.
//!
//! Possession of the signing secret is administrative authority. This helper
//! does not infer spaces, permissions, clearances or subjects from a name.

use std::io::Read;
use std::path::Path;

use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};

use crate::Claims;

/// Provisioning failures without secret or token bytes in diagnostics.
#[derive(Debug, thiserror::Error)]
pub enum ServiceTokenError {
    /// Invalid explicit caller claims or signing configuration.
    #[error("invalid service-token request: {0}")]
    Invalid(&'static str),
    /// Reading a private signing-secret file failed.
    #[error("cannot read service-token signing secret: {0}")]
    SecretFile(#[from] std::io::Error),
    /// JWT serialization/signing failed.
    #[error("service-token signing failed")]
    Signing,
}

/// Read a private, bounded UTF-8 signing-secret file.
///
/// # Errors
/// Refuses nonregular files, Unix group/other access, short secrets,
/// invalid UTF-8 and files exceeding 64 KiB. Only trailing CR/LF are removed.
pub fn read_secret_file(path: &Path) -> Result<String, ServiceTokenError> {
    let file = std::fs::File::open(path)?;
    let metadata = file.metadata()?;
    if !metadata.is_file() {
        return Err(ServiceTokenError::Invalid(
            "secret file must be a regular file",
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o077 != 0 {
            return Err(ServiceTokenError::Invalid(
                "secret file must have no group or other permissions",
            ));
        }
    }
    let mut secret = String::new();
    file.take(65_537).read_to_string(&mut secret)?;
    if secret.len() > 65_536 {
        return Err(ServiceTokenError::Invalid("secret file exceeds 64 KiB"));
    }
    let secret = secret.trim_end_matches(['\r', '\n']).to_owned();
    validate_secret(&secret)?;
    Ok(secret)
}

fn validate_secret(secret: &str) -> Result<(), ServiceTokenError> {
    if secret.trim().len() < 32 {
        return Err(ServiceTokenError::Invalid(
            "signing secret must contain at least 32 bytes",
        ));
    }
    Ok(())
}

fn valid_name(value: &str) -> bool {
    !value.trim().is_empty() && value.trim() == value && !value.chars().any(char::is_control)
}

/// Explicit global management scopes available to a local bootstrap operator.
/// Ordinary data scopes and wildcard scopes are deliberately absent.
pub const GLOBAL_ADMIN_SCOPES: &[&str] = &[
    "manage:grant",
    "manage:provider",
    "read:space_config",
    "write:space_config",
    "read:status",
    "read:system",
    "write:system",
];

/// Issue a short-lived global bootstrap/snapshot administrator token.
///
/// # Errors
/// Requires no space or data clearance and only explicit management scopes.
/// The separate entrypoint prevents missing space arguments from broadening an
/// ordinary service token. Global administration lasts at most one hour.
pub fn issue_global_admin(
    secret: &str,
    claims: &Claims,
    now: i64,
) -> Result<String, ServiceTokenError> {
    if claims.bootstrap_space.is_some()
        || claims.space_id.is_some()
        || claims.spaces.is_some()
        || claims
            .scopes
            .iter()
            .any(|scope| !GLOBAL_ADMIN_SCOPES.contains(&scope.as_str()))
        || !claims.clearances.is_empty()
        || claims.security_level != 0
        || !claims.security_labels.is_empty()
        || claims.subject_allowlist.is_some()
        || !claims.purpose.is_empty()
        || claims
            .exp
            .checked_sub(now)
            .is_none_or(|seconds| !(1..=3600).contains(&seconds))
    {
        return Err(ServiceTokenError::Invalid("global administration accepts only explicit management scopes, no data grants, and at most one hour"));
    }
    sign_claims(secret, claims, now)
}

/// Sign the exact existing bearer claims using HS256.
///
/// # Errors
/// Requires explicit named space grants, subject, nonempty scope list, bounded
/// expiry (at most 31 days), and valid clearance/subject identifiers. Read-only
/// provisioning rejects every scope outside the `read:` namespace, including
/// wildcards granting writes. Empty subject allowlists remain explicitly empty.
pub fn issue(
    secret: &str,
    claims: &Claims,
    read_only: bool,
    now: i64,
) -> Result<String, ServiceTokenError> {
    if claims.bootstrap_space.is_some() {
        return Err(ServiceTokenError::Invalid(
            "use explicit space bootstrap issuance",
        ));
    }
    validate_secret(secret)?;
    let valid_spaces = claims.spaces.as_ref().map_or_else(
        || {
            claims
                .space_id
                .as_deref()
                .is_some_and(|space| elephant_types::validation::validate_space_id(space).is_ok())
        },
        |spaces| {
            !spaces.is_empty()
                && spaces
                    .iter()
                    .all(|space| elephant_types::validation::validate_space_id(space).is_ok())
                && claims
                    .space_id
                    .as_ref()
                    .is_none_or(|selected| spaces.contains(selected))
        },
    );
    if !valid_name(&claims.sub) || !valid_spaces {
        return Err(ServiceTokenError::Invalid(
            "subject and at least one explicit named space are required",
        ));
    }
    if claims.scopes.is_empty() || claims.scopes.iter().any(|scope| !valid_name(scope)) {
        return Err(ServiceTokenError::Invalid(
            "at least one nonempty explicit scope is required",
        ));
    }
    if read_only
        && claims
            .scopes
            .iter()
            .any(|scope| !scope.starts_with("read:") || scope.len() == 5)
    {
        return Err(ServiceTokenError::Invalid(
            "read-only tokens accept only explicit read: scopes",
        ));
    }
    sign_claims(secret, claims, now)
}

/// Issue explicit, management-only bootstrap authority for one named space.
///
/// # Errors
/// Requires the exact bootstrap profile, declared clearances and at most one
/// hour. HTTP record ABAC still applies; MCP and ordinary data routes reject it.
pub fn issue_space_bootstrap(
    secret: &str,
    claims: &Claims,
    now: i64,
) -> Result<String, ServiceTokenError> {
    crate::bootstrap::validate(claims, now)
        .map_err(|_| ServiceTokenError::Invalid("invalid named-space bootstrap authority"))?;
    sign_claims(secret, claims, now)
}

fn sign_claims(secret: &str, claims: &Claims, now: i64) -> Result<String, ServiceTokenError> {
    validate_secret(secret)?;
    if !valid_name(&claims.sub)
        || claims.scopes.is_empty()
        || claims.scopes.iter().any(|scope| !valid_name(scope))
    {
        return Err(ServiceTokenError::Invalid(
            "subject and explicit scopes are required",
        ));
    }
    let lifetime = claims
        .exp
        .checked_sub(now)
        .ok_or(ServiceTokenError::Invalid("expiry overflow"))?;
    if !(1..=31 * 86_400).contains(&lifetime) || claims.iat != now {
        return Err(ServiceTokenError::Invalid(
            "expiry must be in the next 31 days and issued-at must be current",
        ));
    }
    if claims
        .iss
        .as_deref()
        .is_some_and(|value| !valid_name(value))
        || claims.aud.as_ref().is_some_and(|values| {
            values.is_empty() || values.iter().any(|value| !valid_name(value))
        })
        || claims.security_level < 0
        || claims
            .clearances
            .iter()
            .any(|clearance| !valid_name(&clearance.policy_id) || clearance.max_level < 0)
    {
        return Err(ServiceTokenError::Invalid(
            "issuer, audience, security level or clearance is invalid",
        ));
    }
    encode(
        &Header::new(Algorithm::HS256),
        claims,
        &EncodingKey::from_secret(secret.as_bytes()),
    )
    .map_err(|_| ServiceTokenError::Signing)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use crate::LegacyVerifier;

    fn claims() -> Claims {
        let now = chrono::Utc::now().timestamp();
        Claims {
            sub: "household-reader".into(),
            exp: now + 3600,
            iat: now,
            iss: Some("recall-local".into()),
            aud: Some(vec!["recall".into()]),
            space_id: Some("household".into()),
            spaces: Some(vec!["household".into()]),
            bootstrap_space: None,
            scopes: vec!["read:entity".into()],
            security_level: 0,
            security_labels: vec![],
            clearances: vec![],
            subject_allowlist: Some(vec![]),
            purpose: vec![],
        }
    }

    #[test]
    fn issued_token_passes_shared_verifier_and_preserves_empty_subject_scope() {
        let secret = "fixture-secret-with-at-least-thirty-two-bytes";
        let claims = claims();
        let token = issue(secret, &claims, true, claims.iat).unwrap();
        let verified = LegacyVerifier {
            secret,
            algorithm: Algorithm::HS256,
            issuer: Some("recall-local"),
            audience: Some("recall"),
            mappings: None,
        }
        .verify(&token)
        .unwrap();
        assert_eq!(verified.claims().space_id.as_deref(), Some("household"));
        assert_eq!(verified.claims().subject_allowlist, Some(vec![]));
    }

    #[test]
    fn global_bootstrap_is_explicit_bounded_and_has_no_data_authority() {
        let secret = "fixture-secret-with-at-least-thirty-two-bytes";
        let mut claims = claims();
        claims.space_id = None;
        claims.spaces = None;
        claims.subject_allowlist = None;
        claims.scopes = vec!["manage:grant".into(), "write:space_config".into()];
        assert!(issue(secret, &claims, false, claims.iat).is_err());
        assert!(issue_global_admin(secret, &claims, claims.iat).is_ok());
        for scope in ["*", "read:doc", "write:entity", "manage:wiki"] {
            claims.scopes = vec![scope.into()];
            assert!(issue_global_admin(secret, &claims, claims.iat).is_err());
        }
    }

    #[test]
    fn service_spaces_use_the_same_utf8_bounds_as_ingress() {
        let secret = "fixture-secret-with-at-least-thirty-two-bytes";
        for (space, allowed) in [
            ("a".repeat(200), true),
            ("é".repeat(128), true),
            ("é".repeat(129), false),
            (" padded".into(), false),
            ("space\nname".into(), false),
        ] {
            for legacy in [false, true] {
                let mut claims = claims();
                claims.space_id = Some(space.clone());
                claims.spaces = if legacy {
                    None
                } else {
                    Some(vec![space.clone()])
                };
                assert_eq!(issue(secret, &claims, true, claims.iat).is_ok(), allowed);
            }
        }
    }

    #[test]
    fn no_implicit_authority_or_unbounded_lifetime() {
        let secret = "fixture-secret-with-at-least-thirty-two-bytes";
        for case in ["space", "scope", "wildcard", "write", "expiry", "future"] {
            let mut claims = claims();
            let now = claims.iat;
            match case {
                "space" => claims.spaces = Some(vec![]),
                "scope" => claims.scopes.clear(),
                "wildcard" => claims.scopes = vec!["*".into()],
                "write" => claims.scopes = vec!["write:entity".into()],
                "expiry" => claims.exp = now,
                "future" => claims.exp = now + 32 * 86_400,
                _ => unreachable!(),
            }
            assert!(issue(secret, &claims, true, now).is_err(), "{case}");
        }
    }
}
