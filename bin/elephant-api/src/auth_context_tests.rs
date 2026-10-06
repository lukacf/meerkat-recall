#[tokio::test]
async fn verified_http_context_is_retained_and_invalid_tokens_never_reach_body_sink() {
    use elephant_auth::{ExactId, IssuerMapping, PrincipalKind};
    use std::sync::atomic::{AtomicUsize, Ordering};
    let mappings = Arc::new(
        IssuerMappings::new([IssuerMapping {
            issuer: ExactId::new("test-issuer").unwrap(),
            audience: ExactId::new("elephant-test").unwrap(),
            trust_domain: ExactId::new("people-domain").unwrap(),
            legacy_kind: PrincipalKind::Human,
        }])
        .unwrap(),
    );
    let config = Arc::new(AuthConfig {
        secret: "test-ingress-context-key-0123456789".into(),
        issuer: Some("test-issuer".into()),
        audience: Some("elephant-test".into()),
        ..AuthConfig::default()
    });
    let calls = Arc::new(AtomicUsize::new(0));
    let sink_calls = calls.clone();
    let app = Router::new().route("/private", routing::get(move |
        axum::Extension(p): axum::Extension<Principal>,
        axum::Extension(context): axum::Extension<VerifiedCallerContext>
    | {
        let calls = sink_calls.clone();
        async move {
            assert_eq!(context.claims().sub, p.subject);
            assert_eq!(context.claims().iss.as_deref(), Some("test-issuer"));
            assert_eq!(context.claims().aud.as_deref(), Some(["elephant-test".to_string()].as_slice()));
            assert!(context.claims().exp > chrono::Utc::now().timestamp());
            assert_eq!(context.mapped_identity().unwrap().domain().as_str(), "people-domain");
            calls.fetch_add(1, Ordering::SeqCst);
            "PRIVATE HTTP CONTEXT CANARY"
        }
    })).layer(middleware::from_fn_with_state(config.clone(), jwt_auth))
      .layer(axum::Extension(mappings));
    let mut valid = create_test_claims();
    valid.aud = Some(vec!["elephant-test".into()]);
    for mode in 0..8 {
        let mut claims = valid.clone();
        let mut header = Header::default();
        match mode {
            1 => claims.iss = Some("other-issuer".into()),
            2 => claims.aud = Some(vec!["other-audience".into()]),
            3 => claims.sub = "\n".into(),
            4 => claims.exp -= 7200,
            5 => header.typ = Some(elephant_auth::exchange::EXCHANGE_TOKEN_TYPE.into()),
            _ => {}
        }
        let token = encode(
            &header,
            &claims,
            &EncodingKey::from_secret(config.secret.as_bytes()),
        )
        .unwrap();
        let token = if mode == 5 {
            super::test_support::signed_exchange(&config.secret, "test-issuer", "elephant-test")
        } else if mode >= 6 {
            super::test_support::signed_hybrid(&claims, &config.secret, mode == 6)
        } else {
            token
        };
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/private")
                    .header("Authorization", format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            if mode == 0 {
                StatusCode::OK
            } else {
                StatusCode::UNAUTHORIZED
            }
        );
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        assert_eq!(
            std::str::from_utf8(&body)
                .unwrap()
                .contains("PRIVATE HTTP CONTEXT CANARY"),
            mode == 0
        );
    }
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn public_principal_and_skip_auth_cannot_create_verified_http_context() {
    let app = Router::new()
        .route(
            "/context",
            routing::get(
                |context: Option<axum::Extension<VerifiedCallerContext>>| async move {
                    assert!(context.is_none());
                    "no authenticated context"
                },
            ),
        )
        .layer(middleware::from_fn_with_state(
            Arc::new(AuthConfig {
                skip_auth: true,
                ..AuthConfig::default()
            }),
            jwt_auth,
        ));
    let response = app
        .oneshot(
            Request::builder()
                .uri("/context")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let app = Router::new()
        .route(
            "/context",
            routing::get(
                |context: Option<axum::Extension<VerifiedCallerContext>>| async move {
                    assert!(context.is_none());
                    "no authenticated context"
                },
            ),
        )
        .layer(axum::Extension(Principal {
            subject: "forged".into(),
            ..Principal::default()
        }));
    assert_eq!(
        app.oneshot(
            Request::builder()
                .uri("/context")
                .body(Body::empty())
                .unwrap()
        )
        .await
        .unwrap()
        .status(),
        StatusCode::OK
    );
}
