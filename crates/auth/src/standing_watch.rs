//! Explicit admission for a recurring source watch.
//!
//! This is an in-memory proof of authenticated registration, not a durable grant
//! or permission to fetch/commit. Storage must persist the original ceiling and
//! guard the actual current grant, configuration, watch and source/document rows
//! in each transaction. A valid registration deliberately delegates recurring
//! work: expiration of its ingress JWT does not expire the stored delegation.

use std::collections::HashSet;
use std::fmt;
use std::time::{SystemTime, UNIX_EPOCH};

use elephant_policy::{Clearance, Principal};
use elephant_types::space_grant::{intersect_scopes, SpaceGrant};
use thiserror::Error;

use crate::{projection, VerifiedCallerContext};

const REQUIRED_SCOPES: [&str; 4] = ["read:source", "write:source", "read:doc", "write:doc"];

/// Original ingress provenance. No bearer token or signing material is retained.
#[derive(Debug, Clone)]
pub enum StandingWatchOrigin {
    /// A signature-verified named service established the delegation.
    VerifiedService {
        /// Exact authenticated subject, independent of scheduler actor strings.
        subject: String,
        /// Original verified issuer, when the ingress trust configured one.
        issuer: Option<String>,
        /// Original verified audiences, when present.
        audience: Option<Vec<String>>,
        /// Auditable ingress expiry; not the ongoing delegation lifetime.
        registered_token_expires_at: i64,
    },
    /// An explicitly trusted embedding/library caller established the delegation.
    TrustedLocal,
}

/// Non-serializable registration proof. Raw requests cannot manufacture it.
#[derive(Clone)]
pub struct StandingWatchAdmission {
    principal: Principal,
    origin: StandingWatchOrigin,
    registration_grant: Option<SpaceGrant>,
}

impl fmt::Debug for StandingWatchAdmission {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StandingWatchAdmission")
            .field("protected_authority", &"redacted")
            .finish_non_exhaustive()
    }
}

/// Refusal to establish or exercise bounded recurring authority.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum StandingWatchError {
    /// Registration requires a currently valid authenticated named-space caller.
    #[error("standing watch registration requires a current named-space caller")]
    InvalidCaller,
    /// The currently stored named-space grant is absent, inactive or mismatched.
    #[error("standing watch has no matching active service grant")]
    InvalidGrant,
    /// The projected authority exceeds its original authenticated ceiling.
    #[error("standing watch authority exceeds its original ceiling")]
    ExpandedAuthority,
    /// Both source and document read/write authority are necessary.
    #[error("standing watch requires source and document read/write authority")]
    MissingScope,
}

impl StandingWatchAdmission {
    /// Establish a recurring delegation from actual verified ingress and the
    /// already attenuated principal. Storage must independently capture and
    /// compare `current_grant` in the registration transaction.
    ///
    /// # Errors
    /// Rejects expired ingress, management-only callers, mismatched grants and
    /// any principal projection wider than the original signed/current ceiling.
    pub fn verified(
        caller: &VerifiedCallerContext,
        effective: &Principal,
        current_grant: &SpaceGrant,
    ) -> Result<Self, StandingWatchError> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .ok()
            .and_then(|value| i64::try_from(value.as_secs()).ok())
            .ok_or(StandingWatchError::InvalidCaller)?;
        Self::verified_at(caller, effective, current_grant, now)
    }

    fn verified_at(
        caller: &VerifiedCallerContext,
        effective: &Principal,
        current_grant: &SpaceGrant,
        now: i64,
    ) -> Result<Self, StandingWatchError> {
        let claims = caller.claims();
        validate_principal(effective)?;
        if claims.bootstrap_space.is_some()
            || claims.sub.trim().is_empty()
            || !claims.permits_space(&effective.space_id)
            || claims.exp <= now
            || claims.iat > now
        {
            return Err(StandingWatchError::InvalidCaller);
        }
        matching_grant(effective, &claims.sub, current_grant)?;
        let allowed =
            projection::operation_scopes(&intersect_scopes(&claims.scopes, &current_grant.scopes));
        let signed_clearances = projection::clearances(
            &claims.clearances,
            claims.security_level,
            &claims.security_labels,
        );
        if !scope_subset(&effective.scopes, &allowed)
            || !effective
                .purpose
                .is_subset(&claims.purpose.iter().cloned().collect())
            || !subjects_within(effective, claims.subject_allowlist.as_deref())
            || !effective.clearances.iter().all(|clearance| {
                signed_clearances
                    .iter()
                    // PolicyEngine evaluates only the first matching policy.
                    // A later duplicate must never broaden it.
                    .find(|signed| signed.policy_id == clearance.policy_id)
                    .is_some_and(|signed| clearance_within(clearance, signed))
            })
        {
            return Err(StandingWatchError::ExpandedAuthority);
        }
        Ok(Self {
            principal: effective.clone(),
            origin: StandingWatchOrigin::VerifiedService {
                subject: claims.sub.clone(),
                issuer: claims.iss.clone(),
                audience: claims.aud.clone(),
                registered_token_expires_at: claims.exp,
            },
            registration_grant: Some(current_grant.clone()),
        })
    }

    /// Explicit trusted embedding/library registration. This is never selected
    /// because a public request is missing verified authentication evidence.
    ///
    /// # Errors
    /// Rejects an invalid space, empty/invalid clearance ceiling or missing
    /// source/document operation scopes.
    pub fn trusted_local(effective: &Principal) -> Result<Self, StandingWatchError> {
        validate_principal(effective)?;
        Ok(Self {
            principal: effective.clone(),
            origin: StandingWatchOrigin::TrustedLocal,
            registration_grant: None,
        })
    }

    /// Exact original effective ceiling; never replace it with a later grant.
    #[must_use]
    pub const fn principal(&self) -> &Principal {
        &self.principal
    }

    /// Provenance of the explicit registration constructor.
    #[must_use]
    pub const fn origin(&self) -> &StandingWatchOrigin {
        &self.origin
    }

    /// Exact observed grant that storage must guard at registration commit.
    #[must_use]
    pub const fn registration_grant(&self) -> Option<&SpaceGrant> {
        self.registration_grant.as_ref()
    }
}

/// Attenuate a stored original ceiling using one actual current grant snapshot.
///
/// This pure calculation issues no admission or storage proof. Only the private
/// durable registration owner may supply `original` and `subject`; public JSON,
/// actor strings and a newly broadened principal are not original authority.
/// Callers must retain this exact grant/config observation for the committing
/// transaction and recheck before network fetch/dispatch. JWT expiration is not
/// consulted: current revocation and the original ceiling govern recurring work.
///
/// # Errors
/// Refuses revoked/mismatched grants or a current scope reduction that removes
/// required source/document authority.
pub fn attenuate_current_grant(
    original: &Principal,
    subject: &str,
    current_grant: &SpaceGrant,
) -> Result<Principal, StandingWatchError> {
    validate_principal(original)?;
    matching_grant(original, subject, current_grant)?;
    let mut principal = original.clone();
    let original_scopes: Vec<_> = original.scopes.iter().cloned().collect();
    // The persisted ceiling is already projected by ingress. Normalize the
    // current grant using those same rules, then intersect; never re-project
    // the result and add a scope absent from the original effective ceiling.
    let current_scopes =
        projection::operation_scopes(&current_grant.scopes.iter().cloned().collect::<Vec<_>>())
            .into_iter()
            .collect();
    principal.scopes = intersect_scopes(&original_scopes, &current_scopes)
        .into_iter()
        .collect();
    validate_principal(&principal)?;
    Ok(principal)
}

fn matching_grant(
    principal: &Principal,
    subject: &str,
    grant: &SpaceGrant,
) -> Result<(), StandingWatchError> {
    if !grant.active
        || grant.subject != subject
        || grant.space_id != principal.space_id
        || grant.generation.is_empty()
        || subject.trim().is_empty()
    {
        return Err(StandingWatchError::InvalidGrant);
    }
    Ok(())
}

fn validate_principal(principal: &Principal) -> Result<(), StandingWatchError> {
    if elephant_types::validation::validate_space_id(&principal.space_id).is_err()
        || principal.clearances.is_empty()
        || principal
            .clearances
            .iter()
            .any(|clearance| clearance.policy_id.trim().is_empty() || clearance.max_level < 0)
    {
        return Err(StandingWatchError::InvalidCaller);
    }
    if !REQUIRED_SCOPES
        .iter()
        .all(|scope| principal.scopes.contains(*scope) || principal.scopes.contains("*"))
    {
        return Err(StandingWatchError::MissingScope);
    }
    Ok(())
}

fn scope_subset(proposed: &HashSet<String>, ceiling: &HashSet<String>) -> bool {
    proposed.iter().all(|scope| {
        ceiling.contains(scope) || (ceiling.contains("*") && !scope.starts_with("manage:"))
    })
}

fn clearance_within(proposed: &Clearance, signed: &Clearance) -> bool {
    proposed.policy_id == signed.policy_id
        && proposed.max_level <= signed.max_level
        && proposed.labels.is_subset(&signed.labels)
        && proposed.handling_allow.is_subset(&signed.handling_allow)
}

fn subjects_within(principal: &Principal, ceiling: Option<&[elephant_types::RecordId]>) -> bool {
    match (ceiling, principal.subject_allowlist.as_ref()) {
        (None, _) => true,
        (Some(_), None) => false,
        (Some(signed), Some(proposed)) => proposed.iter().all(|subject| signed.contains(subject)),
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use std::collections::BTreeSet;

    use chrono::Utc;
    use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};

    use super::*;
    use crate::{Claims, LegacyVerifier};

    fn fixture() -> (Claims, Principal, SpaceGrant) {
        let scopes: HashSet<String> = REQUIRED_SCOPES.iter().map(ToString::to_string).collect();
        let clearance = Clearance {
            policy_id: "household-policy".into(),
            max_level: 3,
            labels: HashSet::from(["private".into()]),
            handling_allow: HashSet::from(["local".into()]),
        };
        let claims = Claims {
            sub: "watch-service".into(),
            exp: Utc::now().timestamp() + 3600,
            iat: Utc::now().timestamp(),
            iss: None,
            aud: None,
            space_id: Some("household".into()),
            spaces: Some(vec!["household".into()]),
            bootstrap_space: None,
            scopes: scopes.iter().cloned().collect(),
            security_level: 0,
            security_labels: vec![],
            clearances: vec![clearance.clone()],
            subject_allowlist: Some(vec![]),
            purpose: vec!["watch-refresh".into()],
        };
        let principal = Principal {
            space_id: "household".into(),
            scopes: scopes.clone(),
            clearances: vec![clearance],
            subject_allowlist: Some(HashSet::new()),
            purpose: HashSet::from(["watch-refresh".into()]),
        };
        let grant = SpaceGrant {
            space_id: "household".into(),
            subject: "watch-service".into(),
            generation: "original-grant".into(),
            active: true,
            scopes: scopes.into_iter().collect(),
            updated_by: "operator".into(),
            updated_at: Utc::now(),
        };
        (claims, principal, grant)
    }

    fn verify(claims: &Claims) -> VerifiedCallerContext {
        let key = "standing-watch-unit-test-signing-material";
        let token = encode(
            &Header::default(),
            claims,
            &EncodingKey::from_secret(key.as_bytes()),
        )
        .unwrap();
        LegacyVerifier {
            secret: key,
            algorithm: Algorithm::HS256,
            issuer: None,
            audience: None,
            mappings: None,
        }
        .verify(&token)
        .unwrap()
    }

    #[test]
    fn registration_retains_exact_verified_subject_grant_and_effective_ceiling() {
        let (claims, principal, grant) = fixture();
        let admission =
            StandingWatchAdmission::verified(&verify(&claims), &principal, &grant).unwrap();
        assert_eq!(admission.registration_grant(), Some(&grant));
        assert_eq!(admission.principal().scopes, principal.scopes);
        assert_eq!(
            admission.principal().subject_allowlist,
            principal.subject_allowlist
        );
        assert!(
            matches!(admission.origin(), StandingWatchOrigin::VerifiedService {subject, ..} if subject == "watch-service")
        );
        assert!(!format!("{admission:?}").contains("watch-service"));
    }

    #[test]
    fn missing_source_or_document_scopes_refuse_registration_and_local_admission() {
        let (claims, principal, grant) = fixture();
        let caller = verify(&claims);
        for scope in REQUIRED_SCOPES {
            let mut denied = principal.clone();
            denied.scopes.remove(scope);
            assert_eq!(
                StandingWatchAdmission::verified(&caller, &denied, &grant).unwrap_err(),
                StandingWatchError::MissingScope
            );
            assert_eq!(
                StandingWatchAdmission::trusted_local(&denied).unwrap_err(),
                StandingWatchError::MissingScope
            );
        }
    }

    #[test]
    fn service_registration_refuses_wrong_or_revoked_grant() {
        let (claims, principal, grant) = fixture();
        let caller = verify(&claims);
        for case in ["subject", "space", "generation", "inactive"] {
            let mut denied = grant.clone();
            match case {
                "subject" => denied.subject = "other-service".into(),
                "space" => denied.space_id = "business".into(),
                "generation" => denied.generation.clear(),
                _ => denied.active = false,
            }
            assert_eq!(
                StandingWatchAdmission::verified(&caller, &principal, &denied).unwrap_err(),
                StandingWatchError::InvalidGrant
            );
        }
    }

    #[test]
    fn projected_authority_cannot_expand_any_signed_ceiling() {
        let (claims, principal, grant) = fixture();
        let caller = verify(&claims);
        for case in [
            "scope", "policy", "level", "label", "handling", "subjects", "purpose",
        ] {
            let mut denied = principal.clone();
            match case {
                "scope" => {
                    denied.scopes.insert("manage:wiki".into());
                }
                "policy" => denied.clearances[0].policy_id = "business-policy".into(),
                "level" => denied.clearances[0].max_level = 7,
                "label" => {
                    denied.clearances[0].labels.insert("other".into());
                }
                "handling" => {
                    denied.clearances[0].handling_allow.insert("other".into());
                }
                "subjects" => denied.subject_allowlist = None,
                _ => {
                    denied.purpose.insert("other".into());
                }
            }
            assert_eq!(
                StandingWatchAdmission::verified(&caller, &denied, &grant).unwrap_err(),
                StandingWatchError::ExpandedAuthority
            );
        }
    }

    #[test]
    fn duplicate_policy_projection_respects_first_signed_clearance() {
        let (mut claims, mut principal, grant) = fixture();
        let mut later = claims.clearances[0].clone();
        later.max_level = 7;
        later.labels.insert("later-only".into());
        later.handling_allow.insert("later-only".into());
        claims.clearances.push(later.clone());
        let caller = verify(&claims);

        // The signed policy engine still selects the original level 3 entry.
        // Preserve ordinary admission at that ceiling despite the duplicate.
        assert!(StandingWatchAdmission::verified(&caller, &principal, &grant).is_ok());
        for case in ["level", "label", "handling", "replacement"] {
            let mut projected = principal.clone();
            match case {
                "level" => projected.clearances[0].max_level = 7,
                "label" => {
                    projected.clearances[0].labels.insert("later-only".into());
                }
                "handling" => {
                    projected.clearances[0]
                        .handling_allow
                        .insert("later-only".into());
                }
                _ => projected.clearances = vec![later.clone()],
            }
            assert_eq!(
                StandingWatchAdmission::verified(&caller, &projected, &grant).unwrap_err(),
                StandingWatchError::ExpandedAuthority
            );
        }

        // Even a currently shadowed stronger effective entry cannot become a
        // stored ceiling that another consumer might subsequently reorder.
        principal.clearances.push(later);
        assert_eq!(
            StandingWatchAdmission::verified(&caller, &principal, &grant).unwrap_err(),
            StandingWatchError::ExpandedAuthority
        );
    }

    #[test]
    fn ingress_projection_retains_aliases_and_both_legacy_policy_ceilings() {
        let (mut claims, mut principal, mut grant) = fixture();
        claims.clearances.clear();
        claims.security_level = 3;
        claims.security_labels = vec!["private".into()];
        claims.scopes.extend([
            "read:source:raw".into(),
            "read:doc:raw".into(),
            "read:timeline".into(),
            "write:event".into(),
        ]);
        grant.scopes = claims.scopes.iter().cloned().collect();
        principal.scopes = projection::operation_scopes(&claims.scopes);
        principal.clearances = projection::clearances(
            &claims.clearances,
            claims.security_level,
            &claims.security_labels,
        );
        let caller = verify(&claims);
        let admission = StandingWatchAdmission::verified(&caller, &principal, &grant).unwrap();
        assert!(admission.principal().scopes.contains("read:doc:metadata"));
        assert!(admission.principal().scopes.contains("read:event"));
        assert!(admission.principal().scopes.contains("write:timeline"));
        assert_eq!(admission.principal().clearances.len(), 2);
        let recurring =
            attenuate_current_grant(admission.principal(), &claims.sub, &grant).unwrap();
        assert_eq!(recurring.scopes, principal.scopes);

        for case in [
            "default-handling",
            "policy",
            "level",
            "label",
            "tenant-handling",
        ] {
            let mut denied = principal.clone();
            match case {
                "default-handling" => {
                    denied.clearances[0]
                        .handling_allow
                        .insert("handling:trusted_context".into());
                }
                "policy" => denied.clearances[1].policy_id = "unsigned-policy".into(),
                "level" => denied.clearances[1].max_level = 4,
                "label" => {
                    denied.clearances[1].labels.insert("unsigned".into());
                }
                _ => {
                    denied.clearances[1]
                        .handling_allow
                        .insert("unsigned".into());
                }
            }
            assert_eq!(
                StandingWatchAdmission::verified(&caller, &denied, &grant).unwrap_err(),
                StandingWatchError::ExpandedAuthority
            );
        }
    }

    #[test]
    fn projection_follows_raw_ingress_intersection_and_never_restores_removed_authority() {
        let (mut claims, mut principal, mut grant) = fixture();
        // Aliases must not bridge differently spelled raw token/grant scopes at
        // ingress: the real middleware intersects before policy projection.
        claims.scopes.retain(|scope| scope != "read:doc");
        claims.scopes.push("read:doc:metadata".into());
        principal.scopes = projection::operation_scopes(&claims.scopes);
        let caller = verify(&claims);
        assert_eq!(
            StandingWatchAdmission::verified(&caller, &principal, &grant).unwrap_err(),
            StandingWatchError::ExpandedAuthority
        );
        grant.scopes.remove("read:doc");
        grant.scopes.insert("read:doc:metadata".into());
        let admission = StandingWatchAdmission::verified(&caller, &principal, &grant).unwrap();
        let recurring =
            attenuate_current_grant(admission.principal(), &claims.sub, &grant).unwrap();
        assert_eq!(recurring.scopes, principal.scopes);

        // A deliberately narrower original projection stays narrow, even when
        // a later grant adds raw access and management privileges.
        let mut narrower = principal;
        narrower.scopes.remove("read:doc:metadata");
        grant
            .scopes
            .extend(["read:doc:raw".into(), "manage:wiki".into()]);
        let recurring = attenuate_current_grant(&narrower, &claims.sub, &grant).unwrap();
        assert_eq!(recurring.scopes, narrower.scopes);
        grant.scopes.retain(|scope| !scope.starts_with("read:doc"));
        assert_eq!(
            attenuate_current_grant(admission.principal(), &claims.sub, &grant).unwrap_err(),
            StandingWatchError::MissingScope
        );
    }

    #[test]
    fn expiry_blocks_new_registration_but_current_grant_governs_recurring_work() {
        let (claims, principal, grant) = fixture();
        let caller = verify(&claims);
        let admission =
            StandingWatchAdmission::verified_at(&caller, &principal, &grant, claims.exp - 1)
                .unwrap();
        assert_eq!(
            StandingWatchAdmission::verified_at(&caller, &principal, &grant, claims.exp)
                .unwrap_err(),
            StandingWatchError::InvalidCaller
        );
        // Later periodic admission uses the original durable ceiling and CURRENT
        // grant, never reuses the expired token as a bearer or invents a refresh.
        assert!(attenuate_current_grant(admission.principal(), &claims.sub, &grant).is_ok());
        let mut revoked = grant;
        revoked.active = false;
        assert_eq!(
            attenuate_current_grant(admission.principal(), &claims.sub, &revoked).unwrap_err(),
            StandingWatchError::InvalidGrant
        );
    }

    #[test]
    fn later_grants_never_expand_original_scope_or_other_authority() {
        let (claims, principal, grant) = fixture();
        let admission =
            StandingWatchAdmission::verified(&verify(&claims), &principal, &grant).unwrap();
        let mut changed = grant;
        changed.generation = "new-current-observation".into();
        changed.scopes = BTreeSet::from(["*".into(), "manage:wiki".into()]);
        let projected =
            attenuate_current_grant(admission.principal(), &claims.sub, &changed).unwrap();
        assert_eq!(projected.scopes, principal.scopes);
        assert_eq!(projected.subject_allowlist, principal.subject_allowlist);
        assert_eq!(projected.purpose, principal.purpose);
        assert_eq!(projected.clearances[0].max_level, 3);
        changed.scopes = BTreeSet::from(["read:source".into()]);
        assert_eq!(
            attenuate_current_grant(admission.principal(), &claims.sub, &changed).unwrap_err(),
            StandingWatchError::MissingScope
        );
    }

    #[test]
    fn global_or_wrong_named_space_caller_cannot_become_local_authority() {
        let (claims, principal, grant) = fixture();
        for selected in [None, Some("business")] {
            let mut denied = claims.clone();
            denied.space_id = selected.map(str::to_string);
            denied.spaces = Some(selected.into_iter().map(str::to_string).collect());
            let caller = verify(&denied);
            assert_eq!(
                StandingWatchAdmission::verified(&caller, &principal, &grant).unwrap_err(),
                StandingWatchError::InvalidCaller
            );
        }
    }

    #[test]
    fn trusted_local_origin_requires_its_own_explicit_constructor() {
        let (_, principal, _) = fixture();
        let admission = StandingWatchAdmission::trusted_local(&principal).unwrap();
        assert!(matches!(
            admission.origin(),
            StandingWatchOrigin::TrustedLocal
        ));
        assert!(admission.registration_grant().is_none());
    }
}
