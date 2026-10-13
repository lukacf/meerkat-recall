//! Shared fixture for a validly signed and exactly bound exchange.
//!
//! It proves authentication in its verifier but must fail on legacy surfaces.

#![allow(clippy::unwrap_used, clippy::expect_used)]

pub fn signed_exchange(secret: &str, issuer: &str, audience: &str) -> String {
    use elephant_auth::exchange::{
        ArgumentDigest, ExchangeClaims, ExchangeVerifier, EXCHANGE_TOKEN_TYPE,
    };
    use elephant_auth::{ExactId, IssuerMapping, IssuerMappings, PrincipalKind};
    use jsonwebtoken::{encode, EncodingKey, Header};
    let now = chrono::Utc::now().timestamp().cast_unsigned();
    let actor = serde_json::json!({"kind":"human","id":"test-user","qualification":{"kind":"qualified","trust_domain_id":"people-domain"}});
    let claims: ExchangeClaims = serde_json::from_value(serde_json::json!({
        "iss":issuer,"aud":audience,"sub":"test-user","iat":now,"nbf":now,"exp":now+30,"jti":"request-1",
        "version":"v1","profile":"governed_buffered_v1","actor":actor,"requester":actor,"delegation":[],
        "operation":{"operation_id":"request-1","transport":"mcp","operation_name":"get_entity",
            "resource_authority":{"kind":"service_account","id":"elephant","qualification":{"kind":"qualified","trust_domain_id":"elephant-domain"}},
            "space_id":"default","action":"read:entity","resources":["entity:one"],"destination":"host-1","audience":[actor],"purpose":["answer"],
            "arguments_digest":ArgumentDigest::of_bytes(b"fixture-arguments")},
        "revocation":{"authority":actor,"reference":"observation-1","checked_at":now,"valid_until":now+15}
    })).unwrap();
    let issuer = ExactId::new(issuer).unwrap();
    let audience = ExactId::new(audience).unwrap();
    let key = ExactId::new("key-1").unwrap();
    let mappings = IssuerMappings::new([IssuerMapping {
        issuer: issuer.clone(),
        audience: audience.clone(),
        trust_domain: ExactId::new("people-domain").unwrap(),
        legacy_kind: PrincipalKind::Human,
    }])
    .unwrap();
    let header = Header {
        typ: Some(EXCHANGE_TOKEN_TYPE.into()),
        kid: Some(key.as_str().into()),
        ..Header::default()
    };
    let token = encode(
        &header,
        &claims,
        &EncodingKey::from_secret(secret.as_bytes()),
    )
    .unwrap();
    ExchangeVerifier {
        secret,
        key_id: &key,
        issuer: &issuer,
        audience: &audience,
        mappings: &mappings,
        max_lifetime_seconds: 30,
        max_clock_skew_seconds: 0,
        max_observation_seconds: 15,
    }
    .verify(&token, &claims.operation)
    .expect("fixture must be valid before refusal at the legacy sink");
    token
}

pub fn signed_hybrid(claims: &super::Claims, secret: &str, explicit_type: bool) -> String {
    use jsonwebtoken::{encode, EncodingKey, Header};
    let header = Header {
        typ: explicit_type.then(|| "JWT".into()),
        ..Header::default()
    };
    let mut hybrid = serde_json::to_value(claims).unwrap();
    hybrid["profile"] = serde_json::json!("governed_buffered_v1");
    hybrid["operation"] = serde_json::json!({"resources": [], "purpose": ["forbidden"]});
    encode(
        &header,
        &hybrid,
        &EncodingKey::from_secret(secret.as_bytes()),
    )
    .unwrap()
}
