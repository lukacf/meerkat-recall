fn ingress_mappings() -> Arc<IssuerMappings> {
    use elephant_auth::{ExactId, IssuerMapping, PrincipalKind};
    Arc::new(
        IssuerMappings::new([IssuerMapping {
            issuer: ExactId::new("meerkat-test").unwrap(),
            audience: ExactId::new("elephant-test").unwrap(),
            trust_domain: ExactId::new("people-domain").unwrap(),
            legacy_kind: PrincipalKind::Human,
        }])
        .unwrap(),
    )
}

#[tokio::test]
async fn dispatcher_retains_each_authenticated_caller_context(
) -> Result<(), Box<dyn std::error::Error>> {
    let storage = StorageClient::new_embedded().await?;
    let state = McpState::new_with_defaults(
        storage,
        AuthConfig {
            secret: "test-context-key-01234567890123456789".into(),
            issuer: Some("meerkat-test".into()),
            audience: Some("elephant-test".into()),
            ..AuthConfig::default()
        },
        "default".into(),
        vec!["default".into()],
    )
    .with_issuer_mappings(ingress_mappings());
    for subject in ["alice", "bob"] {
        let mut claims = make_claims(Some("default"), &["read:entity"]);
        claims.sub = subject.into();
        claims.iss = Some("meerkat-test".into());
        claims.aud = Some(vec!["elephant-test".into()]);
        let token = encode(
            &Header::default(),
            &claims,
            &EncodingKey::from_secret(state.auth.secret.as_bytes()),
        )?;
        let caller = verified_principal(&state, &token).expect("verified caller");
        let dispatcher = dispatcher_for(&state, caller, DispatcherProfile::Default)
            .await
            .unwrap();
        let retained = dispatcher.context().verified_caller().unwrap();
        assert_eq!(retained.claims().sub, subject);
        assert_eq!(retained.claims().iss, claims.iss);
        assert_eq!(retained.claims().aud, claims.aud);
        assert_eq!(retained.claims().exp, claims.exp);
        assert_eq!(
            retained.mapped_identity().unwrap().domain().as_str(),
            "people-domain"
        );
        assert_eq!(dispatcher.context().principal.space_id, "default");
    }
    let dispatcher = dispatcher_for(
        &state,
        McpRequestPrincipal {
            principal: default_principal(&state),
            verified: None,
        },
        DispatcherProfile::Default,
    )
    .await
    .unwrap();
    assert!(dispatcher.context().verified_caller().is_none());
    Ok(())
}

#[tokio::test]
async fn authenticated_mcp_refuses_wrong_identity_and_exchange_tokens_at_real_body_sink(
) -> Result<(), Box<dyn std::error::Error>> {
    use elephant_storage::Repository;
    let storage = StorageClient::new_embedded().await?;
    let entity = elephant_storage::EntityRepository::new(storage.clone())
        .create(Entity::new(
            base_fields(TableName::Entity, "default", "fixture"),
            EntityType::new("person"),
            "PRIVATE FEDERATION BODY CANARY".into(),
        ))
        .await?;
    let signing_key = "test-context-key-01234567890123456789";
    let state = Arc::new(
        McpState::new_with_defaults(
            storage,
            AuthConfig {
                secret: signing_key.into(),
                issuer: Some("meerkat-test".into()),
                audience: Some("elephant-test".into()),
                ..AuthConfig::default()
            },
            "default".into(),
            vec!["default".into()],
        )
        .with_issuer_mappings(ingress_mappings()),
    );
    let router = mcp_routes(state.clone())
        .merge(mcp_full_routes(state.clone()))
        .merge(mcp_agent_routes(state.clone()))
        .merge(mcp_maintenance_routes(state));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let server = tokio::spawn(async move { axum::serve(listener, router).await });
    for endpoint in ["/mcp", "/mcp/full", "/mcp/agent", "/mcp/maintenance"] {
        for mode in 0..8 {
            let mut claims = make_claims(Some("default"), &["read:entity"]);
            claims.iss = Some("meerkat-test".into());
            claims.aud = Some(vec!["elephant-test".into()]);
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
                &EncodingKey::from_secret(signing_key.as_bytes()),
            )?;
            let token = if mode == 5 {
                crate::auth::test_support::signed_exchange(
                    signing_key,
                    "meerkat-test",
                    "elephant-test",
                )
            } else if mode >= 6 {
                crate::auth::test_support::signed_hybrid(&claims, signing_key, mode == 6)
            } else {
                token
            };
            let config =
                StreamableHttpClientTransportConfig::with_uri(format!("http://{addr}{endpoint}"))
                    .auth_header(token);
            let client = ().serve(StreamableHttpClientTransport::from_config(config)).await?;
            let response = client
                .call_tool(
                    CallToolRequestParams::new("get_entity").with_arguments(
                        serde_json::json!({"id":entity.base.id.to_string()})
                            .as_object()
                            .unwrap()
                            .clone(),
                    ),
                )
                .await?;
            let text = serde_json::to_string(&response)?;
            assert_eq!(
                text.contains("PRIVATE FEDERATION BODY CANARY"),
                mode == 0,
                "{endpoint} mode {mode}: {text}"
            );
            assert_eq!(response.is_error.unwrap_or(false), mode != 0);
            client.cancel().await?;
        }
    }
    server.abort();
    let _ = server.await;
    Ok(())
}
