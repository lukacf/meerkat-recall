#![allow(clippy::unwrap_used, clippy::expect_used)]

use super::*;
use jsonwebtoken::{encode, EncodingKey, Header};
use serde_json::{json, Value};

const JWKS: &[u8] = include_bytes!("../../tests/fixtures/test-public.jwks.json");
const OLD: &[u8] = include_bytes!("../../tests/fixtures/test-old.der");
const NEW: &[u8] = include_bytes!("../../tests/fixtures/test-new.der");

fn claims() -> Value {
    json!({"sub":"service:test", "exp":chrono::Utc::now().timestamp()+600,
        "iss":"operator", "aud":["recall"], "spaces":["alpha"],
        "scopes":["read:doc"], "purpose":["test"]})
}

fn sign(claims: &Value, kid: &str, key: &[u8]) -> String {
    let mut header = Header::new(Algorithm::RS256);
    header.kid = Some(kid.into());
    encode(&header, claims, &EncodingKey::from_rsa_der(key)).unwrap()
}

#[test]
fn rotation_overlap_and_key_removal_are_explicit() {
    let both = TrustedJwks::from_json(JWKS).unwrap();
    let old = sign(&claims(), "test-old", OLD);
    let new = sign(&claims(), "test-new", NEW);
    for token in [&old, &new] {
        let context = both.verify(token, "operator", "recall", None).unwrap();
        assert_eq!(context.claims().sub, "service:test");
        assert_eq!(context.claims().spaces.as_ref().unwrap(), &["alpha"]);
        assert_eq!(context.claims().scopes, ["read:doc"]);
    }
    let mut set: Value = serde_json::from_slice(JWKS).unwrap();
    set["keys"].as_array_mut().unwrap().remove(0);
    let rotated = TrustedJwks::from_json(&serde_json::to_vec(&set).unwrap()).unwrap();
    assert!(rotated.verify(&old, "operator", "recall", None).is_err());
    assert!(rotated.verify(&new, "operator", "recall", None).is_ok());
    assert!(both.verify(&old, "operator", "recall", None).is_ok());
}

#[test]
fn signature_and_key_confusion_never_fall_back() {
    let keys = TrustedJwks::from_json(JWKS).unwrap();
    for (kid, key) in [("unknown", OLD), ("test-old", NEW), ("", OLD)] {
        assert!(keys
            .verify(&sign(&claims(), kid, key), "operator", "recall", None)
            .is_err());
    }
    for algorithm in [Algorithm::HS256, Algorithm::HS384, Algorithm::HS512] {
        let mut header = Header::new(algorithm);
        header.kid = Some("test-old".into());
        let forged = encode(&header, &claims(), &EncodingKey::from_secret(JWKS)).unwrap();
        assert!(keys.verify(&forged, "operator", "recall", None).is_err());
    }
    let mut header = Header::new(Algorithm::RS256);
    header.kid = Some("test-old".into());
    header.jku = Some("http://127.0.0.1/attacker-keys".into());
    let token = encode(&header, &claims(), &EncodingKey::from_rsa_der(OLD)).unwrap();
    assert!(keys.verify(&token, "operator", "recall", None).is_err());
}

#[test]
fn identity_time_and_reserved_profiles_fail_closed() {
    let keys = TrustedJwks::from_json(JWKS).unwrap();
    let now = chrono::Utc::now().timestamp();
    for (field, value) in [
        ("iss", json!("other")),
        ("aud", json!(["other"])),
        ("sub", json!("\n")),
        ("sub", json!("")),
        ("exp", json!(now - 1)),
        ("nbf", json!(now + 600)),
        ("nbf", json!("tomorrow")),
        ("nbf", json!(-1)),
        ("nbf", Value::Null),
        ("profile", Value::Null),
        ("delegation", json!([])),
        ("bootstrap_space", Value::Null),
    ] {
        let mut body = claims();
        body[field] = value;
        assert!(
            keys.verify(&sign(&body, "test-old", OLD), "operator", "recall", None)
                .is_err(),
            "{field}"
        );
    }
    for field in ["iss", "aud", "exp", "sub"] {
        let mut body = claims();
        body.as_object_mut().unwrap().remove(field);
        assert!(
            keys.verify(&sign(&body, "test-old", OLD), "operator", "recall", None)
                .is_err(),
            "{field}"
        );
    }
    let token = sign(&claims(), "test-old", OLD);
    assert!(keys.verify(&token, "", "recall", None).is_err());
    assert!(keys.verify(&token, "operator", "", None).is_err());
}

#[test]
fn configured_keys_reject_private_duplicate_weak_and_wrong_use_material() {
    let original: Value = serde_json::from_slice(JWKS).unwrap();
    for (field, value) in [
        ("d", json!("private")),
        ("kty", json!("oct")),
        ("alg", json!("HS256")),
        ("use", json!("enc")),
        ("key_ops", json!(["sign"])),
        ("kid", json!("")),
        ("kid", json!("bad\nkid")),
        ("n", json!("AQAB")),
        ("e", json!("AQ")),
        ("e", json!("Ag")),
    ] {
        let mut set = original.clone();
        set["keys"][0][field] = value;
        assert!(
            TrustedJwks::from_json(&serde_json::to_vec(&set).unwrap()).is_err(),
            "{field}"
        );
    }
    let mut duplicate = original;
    duplicate["keys"][1] = duplicate["keys"][0].clone();
    assert!(TrustedJwks::from_json(&serde_json::to_vec(&duplicate).unwrap()).is_err());
    assert!(TrustedJwks::from_json(br#"{"keys":[]}"#).is_err());
    assert!(TrustedJwks::from_json(&vec![b' '; MAX_KEY_FILE_BYTES + 1]).is_err());
}

#[test]
fn trusted_mappings_still_bind_the_verified_identity() {
    use crate::{IssuerMapping, PrincipalKind};
    let mappings = IssuerMappings::new([IssuerMapping {
        issuer: ExactId::new("operator").unwrap(),
        audience: ExactId::new("recall").unwrap(),
        trust_domain: ExactId::new("deployment").unwrap(),
        legacy_kind: PrincipalKind::Human,
    }])
    .unwrap();
    let keys = TrustedJwks::from_json(JWKS).unwrap();
    let context = keys
        .verify(
            &sign(&claims(), "test-old", OLD),
            "operator",
            "recall",
            Some(&mappings),
        )
        .unwrap();
    assert_eq!(
        context.mapped_identity().unwrap().domain().as_str(),
        "deployment"
    );
    let mut multi = claims();
    multi["aud"] = json!(["recall", "unrelated"]);
    assert!(keys
        .verify(
            &sign(&multi, "test-old", OLD),
            "operator",
            "recall",
            Some(&mappings)
        )
        .is_err());
}
