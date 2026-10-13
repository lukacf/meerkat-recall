//! Signed transport authentication and live grant attenuation, no bypass principal.
//! Keys/tokens are explicitly synthetic public test fixtures, never real credentials.

#[path = "../../recall_preview/crash_support.rs"]
mod crash_support;
#[path = "../../recall_preview/support.rs"]
mod support;

use axum::{
    body::{to_bytes, Body},
    http::{Request, StatusCode},
    Router,
};
use elephant_api::{
    auth::AuthConfig,
    authenticated_data_routes,
    authz::AuthzService,
    mcp::{mcp_full_routes, McpState},
    runtime::RuntimeManager,
};
use elephant_storage::{
    space_grant_repository::SpaceGrantRepository, Repository, SpaceConfigRepository, StorageClient,
};
use elephant_types::records::base::BaseFields;
use elephant_types::records::SpaceConfig;
use elephant_types::space_grant::SpaceGrant;
use elephant_types::{RecordId, TableName};
use serde_json::{json, Value};
use std::sync::Arc;
use tower::ServiceExt;

fn fixtures() -> anyhow::Result<Value> {
    Ok(serde_json::from_str(include_str!(
        "../../recall_preview/auth_tokens.json"
    ))?)
}

fn auth() -> anyhow::Result<AuthConfig> {
    Ok(AuthConfig {
        secret: fixtures()?["secret"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("synthetic key missing"))?
            .into(),
        algorithm: "HS256".parse()?,
        audience: Some("qualification-api".into()),
        issuer: Some("qualification-only".into()),
        skip_auth: false,
    })
}

fn token(name: &str) -> anyhow::Result<String> {
    fixtures()?["tokens"][name]
        .as_str()
        .map(str::to_owned)
        .ok_or_else(|| anyhow::anyhow!("unknown signed fixture"))
}

async fn register(client: &StorageClient, space: &str) -> anyhow::Result<()> {
    let mut config = serde_json::to_value(BaseFields::new(
        RecordId::new_v4(TableName::SpaceConfig),
        space.into(),
        "qualification".into(),
    ))?;
    config["embedding_provider"] = json!(format!("fixture-{space}"));
    config["embedding_model"] = json!("no-dispatch");
    config["embedding_dims"] = json!(if space == "alpha" { 64 } else { 128 });
    config["processing"] = Value::Null;
    SpaceConfigRepository::new(client.clone())
        .create(serde_json::from_value::<SpaceConfig>(config)?)
        .await?;
    Ok(())
}

async fn grant(client: &StorageClient, space: &str) -> anyhow::Result<SpaceGrant> {
    Ok(SpaceGrantRepository::new(client.clone())
        .replace(
            SpaceGrant {
                space_id: space.into(),
                subject: "qualification-service".into(),
                generation: String::new(),
                active: true,
                scopes: ["read:doc:raw".into()].into(),
                updated_by: "qualification".into(),
                updated_at: chrono::Utc::now(),
            },
            None,
        )
        .await?)
}

async fn rest(
    app: Router,
    space: &str,
    credential: Option<&str>,
    header_space: Option<&str>,
) -> anyhow::Result<(StatusCode, Value)> {
    let mut request = Request::builder()
        .method("POST")
        .uri(format!("/v1/spaces/{space}/search"))
        .header("content-type", "application/json");
    if let Some(credential) = credential {
        request = request.header("authorization", format!("Bearer {credential}"));
    }
    if let Some(selected) = header_space {
        request = request.header("x-space-id", selected);
    }
    let response = app
        .oneshot(request.body(Body::from(serde_json::to_vec(
            &json!({"query":"original auth fixture query","limit":3,"use_vector":false}),
        )?))?)
        .await?;
    let status = response.status();
    let bytes = to_bytes(response.into_body(), 262_144).await?;
    Ok((status, serde_json::from_slice(&bytes)?))
}

#[tokio::test]
async fn scoped_http_checks_signed_authority_and_next_request_live_grants() -> anyhow::Result<()> {
    for backend in support::BACKENDS {
        let client = support::storage(backend).await?;
        register(&client, "alpha").await?;
        register(&client, "beta").await?;
        let grants = SpaceGrantRepository::new(client.clone());
        let mut alpha = grant(&client, "alpha").await?;
        grant(&client, "beta").await?;
        // Exactly the production JWT/runtime/live-grant middleware composition.
        let app = authenticated_data_routes(
            elephant_api::routes::scoped_search::router(
                client.clone(),
                Arc::new(AuthzService::new()),
            ),
            Arc::new(auth()?),
            Arc::new(RuntimeManager::new_preconfigured(client)),
        )?;
        for name in ["valid", "alpha_only"] {
            let (status, body) = rest(app.clone(), "alpha", Some(&token(name)?), None).await?;
            anyhow::ensure!(status == StatusCode::OK && body["results"] == json!([]));
        }
        for name in [
            "expired",
            "wrong_signature",
            "wrong_issuer",
            "wrong_audience",
        ] {
            let (status, _) = rest(app.clone(), "alpha", Some(&token(name)?), None).await?;
            anyhow::ensure!(
                status == StatusCode::UNAUTHORIZED,
                "{backend:?}/{name}: invalid signed bearer accepted"
            );
        }
        anyhow::ensure!(
            rest(app.clone(), "alpha", None, None).await?.0 == StatusCode::UNAUTHORIZED
        );
        for (name, space, header) in [
            ("empty_spaces", "alpha", None),
            ("alpha_only", "beta", None),
            ("valid", "alpha", Some("beta")),
        ] {
            let (status, _) = rest(app.clone(), space, Some(&token(name)?), header).await?;
            anyhow::ensure!(
                status == StatusCode::FORBIDDEN,
                "{backend:?}/{name}: selected space widened signed ceiling or contradicted route"
            );
        }
        let valid = token("valid")?;
        anyhow::ensure!(rest(app.clone(), "beta", Some(&valid), None).await?.0 == StatusCode::OK);
        let previous = alpha.generation.clone();
        alpha.scopes = ["read:source".into()].into();
        alpha = grants.replace(alpha, Some(&previous)).await?;
        anyhow::ensure!(
            rest(app.clone(), "alpha", Some(&valid), None).await?.0 == StatusCode::FORBIDDEN,
            "old signed bearer retained removed raw scope"
        );
        let previous = alpha.generation.clone();
        alpha.scopes = ["read:doc:raw".into()].into();
        alpha.active = false;
        grants.replace(alpha, Some(&previous)).await?;
        anyhow::ensure!(
            rest(app.clone(), "alpha", Some(&valid), None).await?.0 == StatusCode::FORBIDDEN,
            "next HTTP request reused revoked live grant"
        );
        anyhow::ensure!(
            rest(app.clone(), "beta", Some(&valid), None).await?.0 == StatusCode::OK,
            "alpha revocation changed independent beta grant"
        );
    }
    Ok(())
}

#[tokio::test]
async fn retained_source_contact_route_uses_signed_admission_atomic_preflight_and_raw_projection(
) -> anyhow::Result<()> {
    use elephant_api::routes::spaces::{space_routes, SpaceApiState};
    use elephant_storage::{
        EntityIdentityKeyRepository, EntityRepository, RelRepository, SourcePublicationRepository,
        SourceRepository, WorkItemRepository,
    };
    use elephant_types::publication::{OperationReceipt, OperationState};
    use elephant_types::records::base::SubjectsState;
    use elephant_types::records::entity::{Entity, EntityType};
    use elephant_types::records::{Source, SourceType};

    async fn submit(
        app: &Router,
        credential: &str,
        source: &Source,
    ) -> anyhow::Result<(StatusCode, Value)> {
        // The retained route accepts a flattened Source plus top-level controls.
        let mut body = serde_json::to_value(source)?;
        let external = source
            .external_id
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("contact fixture external identity missing"))?;
        body["intake"] =
            json!({"revision_id":"contact-r1","idempotency_key":format!("{external}:r1")});
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/spaces/alpha/sources")
                    .header("content-type", "application/json")
                    .header("authorization", format!("Bearer {credential}"))
                    .body(Body::from(serde_json::to_vec(&body)?))?,
            )
            .await?;
        let status = response.status();
        let body = to_bytes(response.into_body(), 262_144).await?;
        Ok((status, serde_json::from_slice(&body)?))
    }

    for backend in support::BACKENDS {
        let client = support::storage(backend).await?;
        register(&client, "alpha").await?;
        let grants = SpaceGrantRepository::new(client.clone());
        let mut access = grant(&client, "alpha").await?;
        let generation = access.generation.clone();
        access.scopes = ["read:source".into(), "write:source".into()].into();
        access = grants.replace(access, Some(&generation)).await?;
        let app = authenticated_data_routes(
            space_routes().with_state(Arc::new(SpaceApiState::new(
                client.clone(),
                Arc::new(AuthzService::new()),
            ))),
            Arc::new(auth()?),
            Arc::new(RuntimeManager::new_preconfigured(client.clone())),
        )?;
        let credential = token("valid")?;
        let mut source = Source::new(
            BaseFields::new(
                RecordId::new_v4(TableName::Source),
                "alpha".into(),
                "untrusted-body-actor".into(),
            ),
            SourceType::Manual,
            "Structured contact fixture".into(),
        );
        source.base.subjects_state = SubjectsState::Complete;
        source.external_id = Some("qualification/contact-positive".into());
        source.uri = Some("private://qualification/raw-contact-address-book".into());
        source.metadata = json!({"contact":{
            "name":"Qualification Person", "email":"PERSON@EXAMPLE.INVALID",
            "company":"Public Qualification Org", "job_title":"Engineer"
        }});
        let publications = SourcePublicationRepository::new(client.clone());
        let sources = SourceRepository::new(client.clone());
        let entities = EntityRepository::new(client.clone());
        let keys = EntityIdentityKeyRepository::new(client.clone());
        let relationships = RelRepository::new(client.clone());
        let work = WorkItemRepository::new(client.clone());
        let (status, _) = submit(&app, &credential, &source).await?;
        anyhow::ensure!(
            status == StatusCode::FORBIDDEN,
            "source scopes granted implicit contact authority"
        );
        anyhow::ensure!(sources.list("alpha", 100, None).await?.is_empty());
        anyhow::ensure!(entities.list("alpha", 100, None).await?.is_empty());
        anyhow::ensure!(keys.list("alpha", 100, None).await?.is_empty());
        anyhow::ensure!(relationships.list("alpha", 100, None).await?.is_empty());
        anyhow::ensure!(work.list("alpha", 100, None).await?.is_empty());
        anyhow::ensure!(publications
            .external_head(
                "alpha",
                SourceType::Manual,
                "qualification/contact-positive"
            )
            .await?
            .is_none());

        let generation = access.generation.clone();
        access.scopes.extend([
            "read:entity".into(),
            "write:entity".into(),
            "read:relationship".into(),
            "write:relationship".into(),
        ]);
        access = grants.replace(access, Some(&generation)).await?;
        let (status, accepted) = submit(&app, &credential, &source).await?;
        anyhow::ensure!(
            status == StatusCode::ACCEPTED,
            "authorized contact intake did not accept: {accepted}"
        );
        anyhow::ensure!(accepted["source"]["uri"].is_null());
        anyhow::ensure!(accepted["source"]["metadata"].get("_manual_doc").is_none());
        anyhow::ensure!(accepted["operation"].get("source").is_none());
        anyhow::ensure!(!serde_json::to_string(&accepted)?.contains("private://qualification/"));
        let receipt: OperationReceipt = serde_json::from_value(accepted["operation"].clone())?;
        anyhow::ensure!(receipt.state != OperationState::Completed);
        let stored = sources
            .get("alpha", &receipt.source_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("accepted source missing"))?;
        anyhow::ensure!(stored.uri == source.uri);
        anyhow::ensure!(stored.base.created_by == "qualification-service");
        let head = publications
            .head("alpha", &receipt.source_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("accepted head missing"))?;
        anyhow::ensure!(head.published_revision.is_none());
        let work_id: RecordId = accepted["work_item_id"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("original queue receipt missing"))?
            .parse()?;
        let queued = work
            .get("alpha", &work_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("original queued work missing"))?;
        anyhow::ensure!(queued.payload["_publication"]["operation_id"] == receipt.operation_id);
        anyhow::ensure!(queued.payload["_publication"]["generation"] == receipt.source_generation);
        anyhow::ensure!(queued.payload["_publication"]["revision_id"] == "contact-r1");
        // Contact effects are part of whole-source publication, not early writes.
        anyhow::ensure!(entities.list("alpha", 100, None).await?.is_empty());
        anyhow::ensure!(keys.list("alpha", 100, None).await?.is_empty());
        anyhow::ensure!(relationships.list("alpha", 100, None).await?.is_empty());

        let generation = access.generation.clone();
        access.scopes.insert("read:source:raw".into());
        grants.replace(access, Some(&generation)).await?;
        let (status, retry) = submit(&app, &credential, &source).await?;
        anyhow::ensure!(status == StatusCode::ACCEPTED);
        anyhow::ensure!(
            retry["operation"]["operation_id"] == accepted["operation"]["operation_id"]
        );
        anyhow::ensure!(retry["work_item_id"] == accepted["work_item_id"]);
        anyhow::ensure!(retry["source"]["uri"] == json!(source.uri));

        let mut company = Entity::new(
            BaseFields::new(
                RecordId::new_v4(TableName::Entity),
                "alpha".into(),
                "qualification".into(),
            ),
            EntityType::new("org"),
            "Restricted Qualification Org".into(),
        );
        company.base.security.level = 5;
        company.metadata = json!({"private_note":"D-hidden-company-canary"});
        let company = entities.create(company).await?;
        let before_sources = sources.list("alpha", 100, None).await?;
        let before_work = work.list("alpha", 100, None).await?;
        let mut denied_source = source.clone();
        denied_source.base.id = RecordId::new_v4(TableName::Source);
        denied_source.external_id = Some("qualification/contact-denied-dependent".into());
        denied_source.metadata["contact"]["company"] = json!(company.display_name);
        let (status, denied) = submit(&app, &credential, &denied_source).await?;
        anyhow::ensure!(
            status == StatusCode::FORBIDDEN,
            "hidden dependent contact bypassed signed clearance: {denied}"
        );
        anyhow::ensure!(!serde_json::to_string(&denied)?.contains("D-hidden-company-canary"));
        anyhow::ensure!(sources.list("alpha", 100, None).await? == before_sources);
        anyhow::ensure!(work.list("alpha", 100, None).await? == before_work);
        anyhow::ensure!(entities.list("alpha", 100, None).await? == vec![company]);
        anyhow::ensure!(keys.list("alpha", 100, None).await?.is_empty());
        anyhow::ensure!(relationships.list("alpha", 100, None).await?.is_empty());
        anyhow::ensure!(publications
            .external_head(
                "alpha",
                SourceType::Manual,
                "qualification/contact-denied-dependent"
            )
            .await?
            .is_none());
        anyhow::ensure!(publications
            .head("alpha", &denied_source.base.id)
            .await?
            .is_none());
    }
    Ok(())
}

async fn mcp_message(
    app: &Router,
    credential: &str,
    space: &str,
    message: Value,
) -> anyhow::Result<Value> {
    let response = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        app.clone().oneshot(
            Request::builder()
                .method("POST")
                .uri("/mcp/full")
                .header("host", "localhost:3000")
                .header("content-type", "application/json")
                .header("accept", "application/json, text/event-stream")
                .header("authorization", format!("Bearer {credential}"))
                .header("x-space-id", space)
                .body(Body::from(serde_json::to_vec(&message)?))?,
        ),
    )
    .await??;
    anyhow::ensure!(
        response.status().is_success(),
        "MCP transport refused fixture: {}",
        response.status()
    );
    let bytes = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        to_bytes(response.into_body(), 262_144),
    )
    .await??;
    if let Ok(value) = serde_json::from_slice(&bytes) {
        return Ok(value);
    }
    let body = std::str::from_utf8(&bytes)?;
    let values = body
        .lines()
        .filter_map(|line| {
            line.strip_prefix("data: ")
                .or_else(|| line.strip_prefix("data:"))
        })
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    values
        .into_iter()
        .find(|value| value["id"] == message["id"])
        .ok_or_else(|| anyhow::anyhow!("MCP response identity absent"))
}

async fn mcp_search(app: &Router, credential: &str, space: &str) -> anyhow::Result<(bool, Value)> {
    let (is_error, value) = mcp_call(
        app,
        credential,
        space,
        "search_scoped",
        json!({"query":"original auth fixture query","limit":3,"use_vector":false}),
    )
    .await?;
    Ok((
        is_error,
        if is_error {
            value
        } else {
            support::mcp_success_data(value)?
        },
    ))
}

async fn mcp_call(
    app: &Router,
    credential: &str,
    space: &str,
    name: &str,
    arguments: Value,
) -> anyhow::Result<(bool, Value)> {
    let rpc=mcp_message(app,credential,space,json!({"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":name,"arguments":arguments}})).await?;
    anyhow::ensure!(
        rpc.get("error").is_none(),
        "MCP protocol failure instead of typed tool outcome: {rpc}"
    );
    let result = &rpc["result"];
    let text = result["content"]
        .as_array()
        .and_then(|blocks| blocks.iter().find_map(|block| block["text"].as_str()))
        .ok_or_else(|| anyhow::anyhow!("MCP typed tool content absent"))?;
    let is_error = result["isError"] == true;
    let value = serde_json::from_str(text)?;
    Ok((is_error, value))
}

const RUNTIME_CHILD_MODE: &str = "RECALL_QUALIFICATION_RUNTIME_TRANSPORT_CHILD";

/// Public configuration persists a file, so isolate its actual working directory
/// in an owned child instead of changing global cwd in concurrent scenario tests.
#[test]
fn signed_mcp_runtime_gates_mutations_without_disabling_read_only_reads() -> anyhow::Result<()> {
    use std::process::{Child, Command, Stdio};
    use std::time::{Duration, Instant};
    struct Owned(Child);
    impl Drop for Owned {
        fn drop(&mut self) {
            let _kill = self.0.kill();
            let _reap = self.0.wait();
        }
    }
    for backend in ["surreal", "elephant"] {
        let directory = crash_support::CrashDirectory::new()?;
        let error_path = directory.0.join("runtime-child.stderr");
        let mut child = Owned(
            Command::new(std::env::current_exe()?)
                .args(["--exact", "runtime_transport_fixture_child", "--nocapture"])
                .current_dir(&directory.0)
                .env_clear()
                .env(
                    "LD_LIBRARY_PATH",
                    std::env::var_os("LD_LIBRARY_PATH").unwrap_or_default(),
                )
                .env(RUNTIME_CHILD_MODE, backend)
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(std::fs::File::create(&error_path)?)
                .spawn()?,
        );
        let deadline = Instant::now() + Duration::from_secs(45);
        loop {
            if let Some(status) = child.0.try_wait()? {
                anyhow::ensure!(
                    status.success(),
                    "{backend}: runtime transport child failed ({status}): {}",
                    std::fs::read_to_string(&error_path)?
                );
                break;
            }
            anyhow::ensure!(
                Instant::now() < deadline,
                "{backend}: runtime transport child timed out"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
        let receipt: Value =
            serde_json::from_slice(&std::fs::read(directory.0.join("runtime-transport.json"))?)?;
        anyhow::ensure!(
            receipt
                == json!({"backend":backend,"read_only_read":true,"denied_mutations":3,"draining_denials":3})
        );
    }
    Ok(())
}

/// Harness entry point only; the parent case owns its execution and directory.
#[tokio::test]
async fn runtime_transport_fixture_child() -> anyhow::Result<()> {
    let Ok(mode) = std::env::var(RUNTIME_CHILD_MODE) else {
        return Ok(());
    };
    let backend = match mode.as_str() {
        "surreal" => support::Backend::SurrealMemory,
        "elephant" => support::Backend::ElephantDb,
        _ => anyhow::bail!("unknown owned runtime fixture backend"),
    };
    let runtime = Arc::new(RuntimeManager::new_preconfigured(
        support::storage(backend).await?,
    ));
    // No workers or model availability checks are needed for these metadata tools.
    runtime
        .configure_runtime(serde_json::from_value(json!({
            "ingest_enabled":false, "extractor_required":false,
            "snapshot_path":std::env::current_dir()?.join("owned.snapshot")
        }))?)
        .await?;
    let client = runtime
        .current_storage()
        .await
        .ok_or_else(|| anyhow::anyhow!("configured storage absent"))?;
    register(&client, "alpha").await?;
    let mut access = grant(&client, "alpha").await?;
    let prior = access.generation.clone();
    access.scopes = ["*".into()].into();
    SpaceGrantRepository::new(client)
        .replace(access, Some(&prior))
        .await?;
    let app = mcp_full_routes(Arc::new(McpState::new_managed(runtime.clone(), auth()?)));
    let valid = token("valid")?;
    mcp_message(&app,&valid,"alpha",json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-03-26","capabilities":{},"clientInfo":{"name":"D-runtime-oracle","version":"1"}}})).await?;
    const WRITES: [&str; 3] = [
        "restamp_attribution",
        "revert_audit",
        "validate_predicate_proposal",
    ];
    // Each actual registered handler reaches input validation in writable mode.
    for name in WRITES {
        let (error, body) = mcp_call(&app, &valid, "alpha", name, json!({})).await?;
        anyhow::ensure!(
            error && body["error_code"] == "invalid_request",
            "positive dispatch control for {name} failed: {body}"
        );
    }
    runtime
        .configure_runtime(serde_json::from_value(json!({"runtime_mode":"read_only"}))?)
        .await?;
    let listed = mcp_message(
        &app,
        &valid,
        "alpha",
        json!({"jsonrpc":"2.0","id":3,"method":"tools/list","params":{}}),
    )
    .await?;
    let names: std::collections::HashSet<_> = listed["result"]["tools"]
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("tools/list absent"))?
        .iter()
        .filter_map(|tool| tool["name"].as_str())
        .collect();
    anyhow::ensure!(WRITES.iter().all(|name| !names.contains(name)));
    for name in [
        "list_work_items",
        "list_pending_work_items",
        "list_identity_candidates",
    ] {
        anyhow::ensure!(
            names.contains(name),
            "read-only mode hid retained read {name}"
        );
    }
    let (error, body) = mcp_call(
        &app,
        &valid,
        "alpha",
        "list_work_items",
        json!({"limit":10}),
    )
    .await?;
    anyhow::ensure!(
        !error && support::mcp_success_data(body)?["items"] == json!([]),
        "read-only positive read failed"
    );
    for name in WRITES {
        let (error, body) = mcp_call(&app, &valid, "alpha", name, json!({})).await?;
        anyhow::ensure!(
            error && body["error_code"] == "capability_disabled" && body["retryable"] == false,
            "read-only mutation reached input validation/dispatch: {name}: {body}"
        );
    }
    runtime
        .configure_runtime(serde_json::from_value(
            json!({"runtime_mode":"local_memory"}),
        )?)
        .await?;
    runtime.begin_shutdown().await;
    for name in WRITES {
        let (error, body) = mcp_call(&app, &valid, "alpha", name, json!({})).await?;
        anyhow::ensure!(
            error && body["error_code"] == "not_ready",
            "draining mutation reached input validation/dispatch: {name}: {body}"
        );
    }
    std::fs::write(
        "runtime-transport.json",
        serde_json::to_vec(&json!({
            "backend":mode,"read_only_read":true,"denied_mutations":3,"draining_denials":3
        }))?,
    )?;
    Ok(())
}

#[tokio::test]
async fn mcp_http_rechecks_live_grants_and_scope_attenuation_for_each_signed_request(
) -> anyhow::Result<()> {
    for backend in support::BACKENDS {
        let client = support::storage(backend).await?;
        register(&client, "alpha").await?;
        register(&client, "beta").await?;
        let grants = SpaceGrantRepository::new(client.clone());
        let mut alpha = grant(&client, "alpha").await?;
        let app = mcp_full_routes(Arc::new(McpState::new_managed(
            Arc::new(RuntimeManager::new_preconfigured(client)),
            auth()?,
        )));
        let valid = token("valid")?;
        let initialized=mcp_message(&app,&valid,"alpha",json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-03-26","capabilities":{},"clientInfo":{"name":"independent-qualification","version":"1"}}})).await?;
        anyhow::ensure!(initialized["result"]["protocolVersion"].is_string());
        let (error, body) = mcp_search(&app, &valid, "alpha").await?;
        anyhow::ensure!(
            !error && body["results"] == json!([]),
            "valid signed request failed before revocation: {body}"
        );
        let (error, body) = mcp_search(&app, &valid, "beta").await?;
        anyhow::ensure!(
            error && body["error_code"] == "forbidden_space" && body["retryable"] == false,
            "signed ceiling supplied missing live grant"
        );
        for name in ["wrong_signature", "expired"] {
            let (error, body) = mcp_search(&app, &token(name)?, "alpha").await?;
            anyhow::ensure!(
                error && body["error_code"] == "unauthorized" && body["retryable"] == false
            );
        }
        let previous = alpha.generation.clone();
        alpha.scopes = ["read:source".into()].into();
        alpha = grants.replace(alpha, Some(&previous)).await?;
        let (error, body) = mcp_search(&app, &valid, "alpha").await?;
        anyhow::ensure!(
            error && body["error_code"] == "forbidden_space" && body["retryable"] == false,
            "old bearer retained removed raw scope: {body}"
        );
        let previous = alpha.generation.clone();
        alpha.active = false;
        grants.replace(alpha, Some(&previous)).await?;
        let (error, body) = mcp_search(&app, &valid, "alpha").await?;
        anyhow::ensure!(
            error && body["error_code"] == "forbidden_space" && body["retryable"] == false,
            "revoked live grant reused from prior request"
        );
    }
    Ok(())
}
