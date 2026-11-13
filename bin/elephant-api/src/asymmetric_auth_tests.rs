fn public_key_auth_config() -> AuthConfig {
    AuthConfig {
        algorithm: Algorithm::RS256,
        verification_keys: Some(Arc::new(TrustedJwks::from_json(include_bytes!(
            "../../../crates/auth/tests/fixtures/test-public.jwks.json"
        )).unwrap())),
        issuer: Some("test-issuer".into()),
        audience: Some("elephant-test".into()),
        ..AuthConfig::default()
    }
}

#[test]
fn asymmetric_startup_requires_one_key_source_and_fixed_identity_bindings() {
    let valid = public_key_auth_config();
    assert!(valid.validate_server_config().is_ok());
    assert_eq!(AuthConfig::configured_algorithm(Some("RS256")), Ok(Algorithm::RS256));
    for mode in 0..6 {
        let mut config = valid.clone();
        match mode {
            0 => config.secret = "synthetic-key-canary".repeat(2),
            1 => config.verification_keys = None,
            2 => config.issuer = None,
            3 => config.audience = Some(String::new()),
            4 => config.algorithm = Algorithm::HS256,
            _ => config.issuer = Some("\n".into()),
        }
        assert!(config.validate_server_config().is_err());
        assert!(!format!("{config:?}").contains("synthetic-key-canary"));
    }
}

#[tokio::test]
async fn asymmetric_http_rejects_invalid_identity_before_the_body_sink() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let config = Arc::new(public_key_auth_config());
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = calls.clone();
    let app = Router::new().route("/private", routing::get(move |
        axum::Extension(principal): axum::Extension<Principal>,
        axum::Extension(context): axum::Extension<VerifiedCallerContext>
    | {
        let calls = observed.clone();
        async move {
            assert_eq!(context.claims().sub, principal.subject);
            assert_eq!(principal.space_id.as_deref(), Some("default"));
            calls.fetch_add(1, Ordering::SeqCst);
            "ASYMMETRIC PRIVATE BODY CANARY"
        }
    })).layer(middleware::from_fn_with_state(config, jwt_auth));
    for mode in 0..7 {
        let mut claims = create_test_claims();
        claims.space_id = Some("default".into());
        claims.iss = Some("test-issuer".into());
        claims.aud = Some(vec!["elephant-test".into()]);
        let mut header = Header::new(Algorithm::RS256);
        header.kid = Some("test-old".into());
        match mode {
            1 => claims.iss = Some("other".into()),
            2 => claims.aud = None,
            3 => claims.exp = chrono::Utc::now().timestamp()-1,
            4 => header.kid = Some("unknown".into()),
            5 => header.typ = Some(elephant_auth::exchange::EXCHANGE_TOKEN_TYPE.into()),
            6 => header.kid = Some("test-new".into()),
            _ => {}
        }
        let token = encode(&header, &claims, &EncodingKey::from_rsa_der(include_bytes!(
            "../../../crates/auth/tests/fixtures/test-old.der"
        ))).unwrap();
        let response = app.clone().oneshot(Request::builder().uri("/private")
            .header("Authorization", format!("Bearer {token}"))
            .body(Body::empty()).unwrap()).await.unwrap();
        assert_eq!(response.status(), if mode == 0 { StatusCode::OK } else { StatusCode::UNAUTHORIZED });
        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert_eq!(std::str::from_utf8(&body).unwrap().contains("ASYMMETRIC PRIVATE BODY CANARY"), mode == 0);
    }
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}
