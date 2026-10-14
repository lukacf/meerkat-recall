//! Explicit local signing authority for one space's HTTP administration.
//!
//! This profile is not ordinary data authority or a stored service grant. Every
//! ingress validates its signed ceiling; HTTP still authorizes record envelopes.

use crate::{AuthError, Claims};

/// The complete scope ceiling for the short-lived space bootstrap profile.
///
/// Provider management permits declaring processing configuration at creation;
/// HTTP does not admit reindex or other work-producing routes for this profile.
pub const SCOPES: &[&str] = &[
    "read:space_config",
    "write:space_config",
    "manage:grant",
    "manage:provider",
];

/// Validate explicit bootstrap claims after signature verification, or before
/// issuing them with a locally held signing key.
///
/// # Errors
/// Rejects absent authority, widened or inconsistent space/scope ceilings,
/// implicit clearance, subject/purpose projections and non-current lifetimes.
pub fn validate(claims: &Claims, now: i64) -> Result<(), AuthError> {
    let space = claims
        .bootstrap_space
        .as_deref()
        .ok_or(AuthError::UnsupportedProfile)?;
    if elephant_types::validation::validate_space_id(space).is_err()
        || claims.space_id.as_deref() != Some(space)
        || claims.spaces.as_ref().is_none_or(|spaces| {
            spaces.len() != 1 || spaces.first().map(String::as_str) != Some(space)
        })
        || claims.scopes.is_empty()
        || claims
            .scopes
            .iter()
            .any(|scope| !SCOPES.contains(&scope.as_str()))
        || claims.security_level != 0
        || !claims.security_labels.is_empty()
        || claims.clearances.is_empty()
        || claims.clearances.iter().any(|clearance| {
            clearance.policy_id.trim().is_empty()
                || clearance.policy_id.trim() != clearance.policy_id
                || clearance.policy_id.chars().any(char::is_control)
                || clearance.max_level < 0
        })
        || claims.subject_allowlist.is_some()
        || !claims.purpose.is_empty()
    {
        return Err(AuthError::UnsupportedProfile);
    }
    if claims.iat > now
        || claims.exp <= now
        || claims
            .exp
            .checked_sub(claims.iat)
            .is_none_or(|seconds| !(1..=3600).contains(&seconds))
    {
        return Err(AuthError::InvalidFreshness);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{service_token, LegacyVerifier};
    use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
    use serde_json::json;

    #[allow(clippy::unwrap_used)]
    fn claims(now: i64) -> Claims {
        serde_json::from_value(json!({
            "sub":"local-bootstrap", "iat":now, "exp":now+900,
            "space_id":"household", "spaces":["household"],
            "bootstrap_space":"household",
            "scopes":["read:space_config","write:space_config","manage:grant"],
            "clearances":[{"policy_id":"household-policy","max_level":3,"labels":[],"handling_allow":[]}]
        })).unwrap()
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    fn named_bootstrap_uses_separate_issuance_and_shared_signature_verifier() {
        let now = chrono::Utc::now().timestamp();
        let claims = claims(now);
        let secret = "synthetic-only-bootstrap-test-signing-key";
        assert!(service_token::issue(secret, &claims, false, now).is_err());
        assert!(service_token::issue_global_admin(secret, &claims, now).is_err());
        let token = service_token::issue_space_bootstrap(secret, &claims, now).unwrap();
        let verified = LegacyVerifier {
            secret,
            algorithm: Algorithm::HS256,
            issuer: None,
            audience: None,
            mappings: None,
        }
        .verify(&token)
        .unwrap();
        assert_eq!(
            verified.claims().bootstrap_space.as_deref(),
            Some("household")
        );
        assert_eq!(
            verified.claims().clearances[0].policy_id,
            "household-policy"
        );
    }

    #[test]
    #[allow(clippy::unwrap_used, clippy::too_many_lines)]
    fn externally_signed_malformed_bootstrap_is_rejected_at_ingress() {
        let now = chrono::Utc::now().timestamp();
        let secret = "synthetic-only-bootstrap-test-signing-key";
        let verifier = LegacyVerifier {
            secret,
            algorithm: Algorithm::HS256,
            issuer: None,
            audience: None,
            mappings: None,
        };
        for case in [
            "absent_ceiling",
            "empty_ceiling",
            "wide_ceiling",
            "selected_other",
            "no_selected",
            "data_scope",
            "wildcard",
            "empty_scopes",
            "implicit_clearance",
            "level_fallback",
            "labels_fallback",
            "subjects",
            "purpose",
            "future_iat",
            "expired",
            "old_iat",
            "excess_lifetime",
            "negative_clearance",
            "null_profile",
            "empty_profile",
        ] {
            let mut value = serde_json::to_value(claims(now)).unwrap();
            match case {
                "absent_ceiling" => {
                    value.as_object_mut().unwrap().remove("spaces");
                }
                "empty_ceiling" => value["spaces"] = json!([]),
                "wide_ceiling" => value["spaces"] = json!(["household", "business"]),
                "selected_other" => value["space_id"] = json!("business"),
                "no_selected" => value["space_id"] = json!(null),
                "data_scope" => value["scopes"] = json!(["read:doc"]),
                "wildcard" => value["scopes"] = json!(["*"]),
                "empty_scopes" => value["scopes"] = json!([]),
                "implicit_clearance" => value["clearances"] = json!([]),
                "level_fallback" => value["security_level"] = json!(3),
                "labels_fallback" => value["security_labels"] = json!(["private"]),
                "subjects" => value["subject_allowlist"] = json!([]),
                "purpose" => value["purpose"] = json!(["admin"]),
                "future_iat" => value["iat"] = json!(now + 60),
                "expired" => value["exp"] = json!(now - 1),
                "old_iat" => value["iat"] = json!(now - 3600),
                "excess_lifetime" => value["exp"] = json!(now + 3601),
                "negative_clearance" => value["clearances"][0]["max_level"] = json!(-1),
                "null_profile" => value["bootstrap_space"] = json!(null),
                "empty_profile" => value["bootstrap_space"] = json!(""),
                _ => unreachable!(),
            }
            let token = encode(
                &Header::default(),
                &value,
                &EncodingKey::from_secret(secret.as_bytes()),
            )
            .unwrap();
            assert!(verifier.verify(&token).is_err(), "{case}");
        }
    }
}
