#![allow(clippy::unwrap_used, clippy::expect_used, clippy::too_many_lines)]

use super::*;
use crate::exchange::*;
use jsonwebtoken::{encode, EncodingKey, Header};
use serde_json::{json, Value};

const SECRET: &str = "test-only-key-for-elephant-exchange-0123456789";
fn id(value: &str) -> ExactId {
    ExactId::new(value).unwrap()
}
fn principal(value: &str, domain: &str) -> PrincipalClaim {
    PrincipalClaim {
        kind: PrincipalKind::Human,
        id: id(value),
        qualification: QualifiedDomain::Qualified {
            trust_domain_id: id(domain),
        },
    }
}
fn mappings() -> IssuerMappings {
    IssuerMappings::new([IssuerMapping {
        issuer: id("meerkat-issuer"),
        audience: id("elephant-service"),
        trust_domain: id("people-domain"),
        legacy_kind: PrincipalKind::Human,
    }])
    .unwrap()
}
fn legacy_claims() -> Claims {
    Claims {
        sub: "person-canary".into(),
        exp: chrono::Utc::now().timestamp() + 30,
        iat: chrono::Utc::now().timestamp(),
        iss: Some("meerkat-issuer".into()),
        aud: Some(vec!["elephant-service".into()]),
        space_id: Some("space-a".into()),
        scopes: vec!["read:entities".into()],
        security_level: 0,
        security_labels: vec![],
        clearances: vec![],
        subject_allowlist: Some(vec![]),
        purpose: vec![],
    }
}
fn legacy_verifier(mappings: Option<&IssuerMappings>) -> LegacyVerifier<'_> {
    LegacyVerifier {
        secret: SECRET,
        algorithm: Algorithm::HS256,
        issuer: Some("meerkat-issuer"),
        audience: Some("elephant-service"),
        mappings,
    }
}
fn sign<T: serde::Serialize>(claims: &T, exchange: bool) -> String {
    let mut header = Header::default();
    if exchange {
        header.typ = Some(EXCHANGE_TOKEN_TYPE.into());
        header.kid = Some("key-1".into());
    }
    encode(
        &header,
        claims,
        &EncodingKey::from_secret(SECRET.as_bytes()),
    )
    .unwrap()
}
fn operation() -> OperationBinding {
    OperationBinding {
        operation_id: id("request-1"),
        transport: RequestTransport::Mcp,
        operation_name: id("get_entity"),
        resource_authority: principal("elephant", "elephant-domain"),
        space_id: id("space-a"),
        action: id("read:entities"),
        resources: [id("entity:a")].into(),
        destination: id("host-1"),
        audience: [principal("person-canary", "people-domain")].into(),
        purpose: [id("answer")].into(),
        arguments_digest: ArgumentDigest::of_bytes(br#"{"id":"entity:a"}"#),
    }
}
fn exchange_claims(now: u64) -> ExchangeClaims {
    ExchangeClaims {
        iss: id("meerkat-issuer"),
        aud: id("elephant-service"),
        sub: id("person-canary"),
        iat: now,
        nbf: now,
        exp: now + 30,
        jti: id("request-1"),
        version: ExchangeVersion::V1,
        profile: ExchangeProfile::GovernedBufferedV1,
        actor: principal("person-canary", "people-domain"),
        requester: principal("person-canary", "people-domain"),
        delegation: vec![],
        operation: operation(),
        revocation: RevocationObservation {
            authority: principal("grant-owner", "people-domain"),
            reference: id("observation-1"),
            checked_at: now,
            valid_until: now + 15,
        },
    }
}
fn verifier<'a>(
    mappings: &'a IssuerMappings,
    issuer: &'a ExactId,
    audience: &'a ExactId,
    key: &'a ExactId,
) -> ExchangeVerifier<'a> {
    ExchangeVerifier {
        secret: SECRET,
        issuer,
        audience,
        key_id: key,
        mappings,
        max_lifetime_seconds: 30,
        max_clock_skew_seconds: 0,
        max_observation_seconds: 15,
    }
}
fn with_verifier(f: impl FnOnce(&ExchangeVerifier<'_>, u64)) {
    let mappings = mappings();
    let issuer = id("meerkat-issuer");
    let audience = id("elephant-service");
    let key = id("key-1");
    f(
        &verifier(&mappings, &issuer, &audience, &key),
        chrono::Utc::now().timestamp().cast_unsigned(),
    );
}

#[test]
fn legacy_context_preserves_verified_claims_without_invented_identity() {
    let claims = legacy_claims();
    let context = legacy_verifier(None).verify(&sign(&claims, false)).unwrap();
    assert_eq!(context.claims().sub, claims.sub);
    assert_eq!(context.claims().iss, claims.iss);
    assert_eq!(context.claims().aud, claims.aud);
    assert_eq!(context.claims().exp, claims.exp);
    assert_eq!(context.claims().subject_allowlist, Some(vec![]));
    assert!(context.mapped_identity().is_none());
    assert!(!format!("{context:?}").contains("person-canary"));
}

#[test]
fn legacy_preserves_explicit_hmac_algorithms_without_algorithm_confusion() {
    let claims = legacy_claims();
    for algorithm in [Algorithm::HS256, Algorithm::HS384, Algorithm::HS512] {
        let token = encode(
            &Header::new(algorithm),
            &claims,
            &EncodingKey::from_secret(SECRET.as_bytes()),
        )
        .unwrap();
        let mut verifier = legacy_verifier(None);
        verifier.algorithm = algorithm;
        assert!(verifier.verify(&token).is_ok());
        verifier.algorithm = if algorithm == Algorithm::HS256 {
            Algorithm::HS384
        } else {
            Algorithm::HS256
        };
        assert!(verifier.verify(&token).is_err());
    }
}

#[test]
fn reserved_exchange_claims_cannot_downgrade_through_legacy_token_types() {
    let verifier = legacy_verifier(None);
    for token_type in [Some("JWT".to_string()), None] {
        let header = Header {
            typ: token_type,
            ..Header::default()
        };
        let mut ordinary = serde_json::to_value(legacy_claims()).unwrap();
        ordinary["unrelated_legacy_extension"] = json!("retained compatibility");
        let token = encode(
            &header,
            &ordinary,
            &EncodingKey::from_secret(SECRET.as_bytes()),
        )
        .unwrap();
        assert!(verifier.verify(&token).is_ok());
        for field in [
            "version",
            "profile",
            "actor",
            "requester",
            "delegation",
            "operation",
            "revocation",
        ] {
            for value in [Value::Null, json!("governed_buffered_v1"), json!({})] {
                let mut hybrid = ordinary.clone();
                hybrid[field] = value;
                let token = encode(
                    &header,
                    &hybrid,
                    &EncodingKey::from_secret(SECRET.as_bytes()),
                )
                .unwrap();
                assert!(
                    matches!(verifier.verify(&token), Err(AuthError::UnsupportedProfile)),
                    "{field}"
                );
            }
        }
    }
}

#[test]
fn mapped_legacy_identity_requires_exact_independent_configuration() {
    let map = mappings();
    let mut claims = legacy_claims();
    let verifier = legacy_verifier(Some(&map));
    let context = verifier.verify(&sign(&claims, false)).unwrap();
    assert_eq!(
        context.mapped_identity(),
        Some(&principal("person-canary", "people-domain"))
    );
    for values in [
        vec![],
        vec!["elephant-service".into(), "other-service".into()],
    ] {
        claims.aud = Some(values);
        assert!(verifier.verify(&sign(&claims, false)).is_err());
    }
    let mut verifier = legacy_verifier(Some(&map));
    verifier.issuer = None;
    assert!(verifier.verify(&sign(&legacy_claims(), false)).is_err());
}

#[test]
fn issuer_audience_and_key_mismatches_cannot_create_context() {
    let base = legacy_claims();
    let map = mappings();
    let verifier = legacy_verifier(Some(&map));
    for mutate in [
        |c: &mut Claims| c.iss = Some("other".into()),
        |c: &mut Claims| c.aud = Some(vec!["other".into()]),
        |c: &mut Claims| c.exp -= 3600,
        |c: &mut Claims| c.sub = "\n".into(),
    ] {
        let mut claims = base.clone();
        mutate(&mut claims);
        assert!(verifier.verify(&sign(&claims, false)).is_err());
    }
    let bad = encode(
        &Header::default(),
        &base,
        &EncodingKey::from_secret(b"different-key"),
    )
    .unwrap();
    assert!(verifier.verify(&bad).is_err());
}

#[test]
fn mappings_refuse_empty_and_ambiguous_configuration() {
    assert!(IssuerMappings::new([]).is_err());
    let mapping = IssuerMapping {
        issuer: id("issuer"),
        audience: id("audience"),
        trust_domain: id("domain"),
        legacy_kind: PrincipalKind::Human,
    };
    assert!(IssuerMappings::new([mapping.clone(), mapping]).is_err());
}

#[test]
fn qualified_wire_requires_canonical_shape_and_valid_exact_identity() {
    for invalid in ["", " \t ", "\u{2003}", "a\n", "a\u{0085}"] {
        assert!(ExactId::new(invalid).is_err());
    }
    for valid in [" alice ", "\u{feff}", "\u{200b}", "person-😀"] {
        assert!(ExactId::new(valid).is_ok());
    }
    let expected = json!({"kind":"human","id":"alice","qualification":{"kind":"qualified","trust_domain_id":"domain:a"}});
    assert_eq!(
        serde_json::to_value(principal("alice", "domain:a")).unwrap(),
        expected
    );
    assert_ne!(
        principal("alice", "domain:a"),
        principal("alice", "domain:b")
    );
    for qualification in [
        Value::Null,
        json!({"kind":"unqualified"}),
        json!({"kind":"unqualified","trust_domain_id":null}),
        json!({"kind":"qualified","trust_domain_id":""}),
        json!({"kind":"qualified","trust_domain_id":"domain:a","extra":true}),
    ] {
        let mut wire = expected.clone();
        wire["qualification"] = qualification;
        assert!(serde_json::from_value::<PrincipalClaim>(wire).is_err());
    }
}

#[test]
fn correctly_bound_exchange_authenticates_claims_but_cannot_enter_legacy_ingress() {
    with_verifier(|verifier, now| {
        let claims = exchange_claims(now);
        let token = sign(&claims, true);
        let context = verifier.verify_at(&token, &operation(), now).unwrap();
        assert_eq!(context.claims(), &claims);
        assert!(!format!("{context:?}").contains("person-canary"));
        assert_eq!(
            legacy_verifier(None).verify(&token).unwrap_err(),
            AuthError::UnsupportedProfile
        );
        assert!(verifier
            .verify_at(&sign(&legacy_claims(), false), &operation(), now)
            .is_err());
    });
}

#[test]
fn every_operation_binding_change_refuses_before_a_sink() {
    with_verifier(|verifier, now| {
        let claims = exchange_claims(now);
        let token = sign(&claims, true);
        let mut sink = Vec::new();
        let mutations: Vec<fn(&mut OperationBinding)> = vec![
            |b| b.operation_id = id("request-2"),
            |b| b.transport = RequestTransport::Http,
            |b| b.operation_name = id("search_entities"),
            |b| b.resource_authority = principal("elephant", "other-domain"),
            |b| b.space_id = id("space-b"),
            |b| b.action = id("write:entities"),
            |b| b.resources = [id("entity:b")].into(),
            |b| b.destination = id("host-2"),
            |b| b.audience = [principal("other", "people-domain")].into(),
            |b| b.purpose = [id("surveillance")].into(),
            |b| b.arguments_digest = ArgumentDigest::of_bytes(b"changed"),
        ];
        for mutate in mutations {
            let mut observed = operation();
            mutate(&mut observed);
            if verifier.verify_at(&token, &observed, now).is_ok() {
                sink.push("private-body");
            }
        }
        assert!(sink.is_empty());
    });
}

#[test]
fn strict_exchange_refuses_missing_unknown_null_and_malformed_claims() {
    with_verifier(|verifier, now| {
        let valid = serde_json::to_value(exchange_claims(now)).unwrap();
        for field in valid.as_object().unwrap().keys() {
            let mut wire = valid.clone();
            wire.as_object_mut().unwrap().remove(field);
            assert!(
                verifier
                    .verify_at(&sign(&wire, true), &operation(), now)
                    .is_err(),
                "{field}"
            );
        }
        for (field, value) in [
            ("extra", json!(true)),
            ("version", json!("v2")),
            ("profile", json!("trusted_embedded")),
            ("delegation", Value::Null),
            ("clearances", json!([{"max_level":100}])),
            ("sub", json!("different")),
            ("jti", json!("request-2")),
        ] {
            let mut wire = valid.clone();
            wire[field] = value;
            assert!(
                verifier
                    .verify_at(&sign(&wire, true), &operation(), now)
                    .is_err(),
                "{field}"
            );
        }
        let mut wire = valid;
        wire["actor"]["qualification"]["trust_domain_id"] = json!("alien-domain");
        assert!(verifier
            .verify_at(&sign(&wire, true), &operation(), now)
            .is_err());
    });
}

#[test]
fn exchange_freshness_does_not_inherit_legacy_jwt_leeway() {
    with_verifier(|verifier, now| {
        for mutate in [
            |c: &mut ExchangeClaims| c.exp = c.iat,
            |c: &mut ExchangeClaims| c.iat += 1,
            |c: &mut ExchangeClaims| c.nbf += 1,
            |c: &mut ExchangeClaims| c.nbf -= 1,
            |c: &mut ExchangeClaims| c.exp += 1,
            |c: &mut ExchangeClaims| c.revocation.checked_at += 1,
            |c: &mut ExchangeClaims| c.revocation.valid_until = c.iat,
            |c: &mut ExchangeClaims| c.revocation.valid_until += 1,
        ] {
            let mut claims = exchange_claims(now);
            mutate(&mut claims);
            assert!(verifier
                .verify_at(&sign(&claims, true), &operation(), now)
                .is_err());
        }
    });
}

#[test]
fn clock_uncertainty_shrinks_both_ends_of_exchange_acceptance() {
    with_verifier(|verifier, _| {
        let mut bounded = ExchangeVerifier {
            max_clock_skew_seconds: 5,
            ..*verifier
        };
        let mut claims = exchange_claims(980);
        claims.nbf = 995;
        claims.revocation.checked_at = 995;
        claims.revocation.valid_until = 1010;
        let token = sign(&claims, true);
        // Local 1000 means real time may be anywhere in inclusive [995,1005].
        assert!(bounded.verify_at(&token, &operation(), 1000).is_ok());
        // A fast receiver must not admit a possibly not-yet-active request.
        assert!(bounded.verify_at(&token, &operation(), 999).is_err());
        // A slow receiver must stop at the earliest possible expiry boundary.
        assert!(bounded.verify_at(&token, &operation(), 1005).is_err());
        claims.revocation.valid_until = 1006;
        assert!(bounded
            .verify_at(&sign(&claims, true), &operation(), 1001)
            .is_err());
        claims.revocation.valid_until = 1010;
        claims.revocation.checked_at = 996;
        assert!(bounded
            .verify_at(&sign(&claims, true), &operation(), 1000)
            .is_err());
        // At zero skew, activation is inclusive and expiration exclusive.
        bounded.max_clock_skew_seconds = 0;
        claims.revocation.checked_at = 995;
        let token = sign(&claims, true);
        assert!(bounded.verify_at(&token, &operation(), 995).is_ok());
        assert!(bounded.verify_at(&token, &operation(), 1010).is_err());
        bounded.max_clock_skew_seconds = 5;
        assert!(bounded.verify_at(&token, &operation(), 0).is_err());
        assert!(bounded.verify_at(&token, &operation(), u64::MAX).is_err());
    });
}

#[test]
fn protected_exchange_diagnostics_redact_identifiers_and_grant_references() {
    let claims = exchange_claims(1000);
    let delegation = DelegationReference {
        authority: principal("grant-canary", "people-domain"),
        reference: id("lineage-canary"),
    };
    for diagnostic in [
        format!("{claims:?}"),
        format!("{:?}", claims.operation),
        format!("{:?}", claims.actor),
        format!("{:?}", claims.actor.qualification),
        format!("{:?}", claims.actor.id),
        format!("{:?}", claims.revocation),
        format!("{delegation:?}"),
        format!("{:?}", claims.operation.arguments_digest),
    ] {
        assert!(diagnostic.contains("redacted"));
        for protected in [
            "person-canary",
            "people-domain",
            "entity:a",
            "grant-canary",
            "lineage-canary",
            "observation-1",
        ] {
            assert!(!diagnostic.contains(protected));
        }
    }
}

#[test]
fn exchange_rejects_header_key_discovery_and_wrong_key_or_algorithm() {
    with_verifier(|verifier, now| {
        let claims = exchange_claims(now);
        for mutate in [
            |h: &mut Header| h.kid = Some("other".into()),
            |h: &mut Header| h.jku = Some("https://untrusted.invalid/key".into()),
            |h: &mut Header| h.x5u = Some("https://untrusted.invalid/cert".into()),
            |h: &mut Header| h.cty = Some("unknown".into()),
            |h: &mut Header| h.alg = Algorithm::HS384,
        ] {
            let mut header = Header {
                typ: Some(EXCHANGE_TOKEN_TYPE.into()),
                kid: Some("key-1".into()),
                ..Header::default()
            };
            mutate(&mut header);
            let token = encode(
                &header,
                &claims,
                &EncodingKey::from_secret(SECRET.as_bytes()),
            )
            .unwrap();
            assert!(verifier.verify_at(&token, &operation(), now).is_err());
        }
    });
}

#[test]
fn qualified_principal_binding_conforms_to_pinned_canonical_schema() {
    let schema: Value = serde_json::from_str(include_str!(
        "../tests/fixtures/meerkat-principal-ref.schema.json"
    ))
    .unwrap();
    let validator = jsonschema::validator_for(&schema).unwrap();
    for kind in [
        PrincipalKind::Human,
        PrincipalKind::PersonalAgent,
        PrincipalKind::SharedAgent,
        PrincipalKind::RuntimeHost,
        PrincipalKind::ServiceAccount,
    ] {
        for text in ["alice", " alice ", "\u{feff}", "\u{200b}", "person-😀"] {
            let mut value = principal(text, "domain:a");
            value.kind = kind;
            let wire = serde_json::to_value(&value).unwrap();
            assert!(validator.is_valid(&wire));
            assert_eq!(
                serde_json::from_value::<PrincipalClaim>(wire).unwrap(),
                value
            );
        }
    }
    for invalid in ["", "\t", "\u{2003}", "alice\n", "alice\u{0085}"] {
        for field in ["id", "domain"] {
            let mut wire = serde_json::to_value(principal("alice", "domain:a")).unwrap();
            if field == "id" {
                wire["id"] = json!(invalid);
            } else {
                wire["qualification"]["trust_domain_id"] = json!(invalid);
            }
            assert!(!validator.is_valid(&wire));
            assert!(serde_json::from_value::<PrincipalClaim>(wire).is_err());
        }
    }
    let legacy = json!({"kind":"human","id":"alice"});
    assert!(validator.is_valid(&legacy));
    assert!(serde_json::from_value::<PrincipalClaim>(legacy).is_err());
}
