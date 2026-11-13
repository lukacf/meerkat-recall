#[tokio::test]
async fn asymmetric_mcp_keeps_real_body_and_current_grant_checks() -> Result<(), Box<dyn std::error::Error>> {
    use elephant_storage::{Repository, SpaceConfigRepository};
    use elephant_storage::space_grant_repository::SpaceGrantRepository;
    use elephant_types::{records::SpaceConfig, space_grant::SpaceGrant};
    use std::collections::BTreeSet;

    let storage = StorageClient::new_embedded().await?;
    let space: SpaceConfig = serde_json::from_value(serde_json::json!({
        "id":RecordId::new_v4(TableName::SpaceConfig), "space_id":"default",
        "created_at":chrono::Utc::now(), "updated_at":chrono::Utc::now(), "created_by":"fixture",
        "security":{"policy_id":"default","level":0,"labels":[],"handling":[]},
        "provenance":{}, "subjects":[], "subjects_state":"complete",
        "embedding_provider":"fake", "embedding_model":"fixture", "embedding_dims":3,
        "purpose":null,"skill":null
    }))?;
    SpaceConfigRepository::new(storage.clone()).create(space).await?;
    let entity = elephant_storage::EntityRepository::new(storage.clone()).create(Entity::new(
        base_fields(TableName::Entity, "default", "fixture"),
        EntityType::new("person"), "ASYMMETRIC MCP PRIVATE CANARY".into(),
    )).await?;
    let mut claims = make_claims(Some("default"), &["read:entity"]);
    claims.iss = Some("operator".into());
    claims.aud = Some(vec!["recall".into()]);
    claims.spaces = Some(vec!["default".into()]);
    let grants = SpaceGrantRepository::new(storage.clone());
    let mut grant = grants.replace(SpaceGrant {
        space_id: "default".into(), subject: claims.sub.clone(), generation: String::new(),
        active: true, scopes: BTreeSet::from(["read:entity".into()]),
        updated_by: "fixture".into(), updated_at: chrono::Utc::now(),
    }, None).await?;
    let auth = AuthConfig {
        algorithm: Algorithm::RS256,
        verification_keys: Some(Arc::new(elephant_auth::asymmetric::TrustedJwks::from_json(
            include_bytes!("../../../crates/auth/tests/fixtures/test-public.jwks.json")
        )?)),
        issuer: Some("operator".into()), audience: Some("recall".into()),
        ..AuthConfig::default()
    };
    auth.validate_server_config()?;
    let state = Arc::new(McpState::new_with_defaults(storage, auth, "default".into(), vec!["default".into()]));
    let router = mcp_routes(state.clone()).merge(mcp_full_routes(state.clone()))
        .merge(mcp_agent_routes(state.clone())).merge(mcp_maintenance_routes(state));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let server = tokio::spawn(async move { axum::serve(listener, router).await });
    for endpoint in ["/mcp", "/mcp/full", "/mcp/agent", "/mcp/maintenance"] {
        for mode in 0..5 {
            let mut body = claims.clone();
            let mut header = Header::new(Algorithm::RS256);
            header.kid = Some("test-old".into());
            match mode {
                1 => body.iss = Some("other".into()),
                2 => header.kid = Some("unknown".into()),
                3 => body.subject_allowlist = Some(Vec::new()),
                _ => {}
            }
            let token = encode(&header, &body, &EncodingKey::from_rsa_der(include_bytes!(
                "../../../crates/auth/tests/fixtures/test-old.der"
            )))?;
            let config = StreamableHttpClientTransportConfig::with_uri(format!("http://{addr}{endpoint}"))
                .auth_header(token);
            let client = ().serve(StreamableHttpClientTransport::from_config(config)).await?;
            if mode == 4 {
                let admitted = client.call_tool(CallToolRequestParams::new("get_entity").with_arguments(
                    serde_json::json!({"id":entity.base.id.to_string()}).as_object().unwrap().clone()
                )).await?;
                assert!(serde_json::to_string(&admitted)?.contains("ASYMMETRIC MCP PRIVATE CANARY"));
            }
            if mode == 4 {
                let mut revoked = grant.clone();
                revoked.active = false;
                grant = grants.replace(revoked, Some(&grant.generation)).await?;
            }
            let response = client.call_tool(CallToolRequestParams::new("get_entity").with_arguments(
                serde_json::json!({"id":entity.base.id.to_string()}).as_object().unwrap().clone()
            )).await?;
            assert_eq!(serde_json::to_string(&response)?.contains("ASYMMETRIC MCP PRIVATE CANARY"), mode == 0, "{endpoint} mode {mode}");
            assert_eq!(response.is_error.unwrap_or(false), mode != 0);
            client.cancel().await?;
            if mode == 4 {
                let mut restored = grant.clone();
                restored.active = true;
                grant = grants.replace(restored, Some(&grant.generation)).await?;
            }
        }
    }
    server.abort();
    let _ = server.await;
    Ok(())
}
