//! Independent actual HTTP router + MCP dispatcher input/space oracles.
//! Principals are injected at the verified-context boundary; this is not a
//! signed-token, live-grant, HTTP MCP transport or installed-artifact receipt.

#[path = "../../recall_preview/support.rs"]
mod support;

use axum::{
    body::{to_bytes, Body},
    http::{Request, StatusCode},
    Extension,
};
use elephant_api::auth::Principal;
use elephant_api::authz::AuthzService;
use elephant_mcp::{ToolContext, ToolDispatcher};
use elephant_storage::source_publication::{AcceptSourceRevision, SourcePublicationRepository};
use elephant_storage::{Repository, SpaceConfigRepository, StorageClient};
use elephant_types::publication::SourcePrecondition;
use elephant_types::records::base::{BaseFields, SubjectsState};
use elephant_types::records::{ManualDocTemplate, Source, SourceType, SpaceConfig};
use elephant_types::{RecordId, TableName};
use serde_json::{json, Value};
use std::sync::Arc;
use tower::ServiceExt;

fn principal(space: &str, raw: bool) -> Principal {
    Principal {
        subject: "qualification-reader".into(),
        space_id: Some(space.into()),
        scopes: if raw {
            vec!["read:doc:raw".into()]
        } else {
            vec!["read:doc:metadata".into()]
        },
        ..Principal::default()
    }
}

async fn http(
    client: &StorageClient,
    route_space: &str,
    principal_space: &str,
    raw: bool,
    input: Value,
) -> anyhow::Result<(StatusCode, Value)> {
    let app =
        elephant_api::routes::scoped_search::router(client.clone(), Arc::new(AuthzService::new()))
            .layer(Extension(principal(principal_space, raw)));
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/v1/spaces/{route_space}/search"))
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&input)?))?,
        )
        .await?;
    let status = response.status();
    let bytes = to_bytes(response.into_body(), 262_144).await?;
    Ok((status, serde_json::from_slice(&bytes)?))
}

async fn mcp(client: &StorageClient, space: &str, input: Value) -> elephant_mcp::McpResult<Value> {
    let authorization = AuthzService::new();
    let context = ToolContext::new(
        client.clone(),
        authorization.to_abac_principal(&principal(space, true)),
    );
    let mut dispatcher = ToolDispatcher::new(context);
    dispatcher.register(elephant_mcp::scoped_search::ScopedSearchHandler);
    dispatcher.execute("search_scoped", input).await
}

fn public_mcp_error(error: elephant_mcp::McpError) -> anyhow::Result<Value> {
    match error {
        elephant_mcp::McpError::Public(error) => Ok(serde_json::to_value(error)?),
        other => anyhow::bail!("public adapter lost typed error: {other}"),
    }
}

#[tokio::test]
async fn http_and_mcp_enforce_same_search_input_bounds_before_storage_or_models(
) -> anyhow::Result<()> {
    for backend in support::BACKENDS {
        let client = support::storage(backend).await?;
        let constraints: serde_json::Map<String, Value> = (0..101)
            .map(|i| (format!("source-{i}"), json!("r1")))
            .collect();
        for input in [
            json!({"query":""}),
            json!({"query":"   "}),
            json!({"query":"é".repeat(2049),"use_vector":false}),
            json!({"query":"x".repeat(4097),"use_vector":false}),
            json!({"query":"valid","limit":0}),
            json!({"query":"valid","limit":51}),
            json!({"query":"valid","source_revision_constraints":[]}),
            json!({"query":"valid","source_revision_constraints":constraints}),
            json!({"query":"valid","filters":{"labels":{"initiative":[]}}}),
            json!({"query":"valid","space_id":"ungranted-space"}),
        ] {
            let (status, http_error) = http(&client, "alpha", "alpha", true, input.clone()).await?;
            anyhow::ensure!(
                status == StatusCode::BAD_REQUEST && http_error["error_code"] == "invalid_request"
            );
            anyhow::ensure!(http_error["retryable"] == false);
            let error = mcp(&client, "alpha", input)
                .await
                .err()
                .ok_or_else(|| anyhow::anyhow!("invalid MCP request succeeded"))?;
            let mcp_error = public_mcp_error(error)?;
            anyhow::ensure!(
                mcp_error["error_code"] == http_error["error_code"]
                    && mcp_error["retryable"] == false
            );
        }
        // A exactly-bound query passes validation and reaches actual space lookup.
        for query in ["x".repeat(4096), "é".repeat(2048)] {
            let input = json!({"query":query,"use_vector":false});
            let (status, body) = http(&client, "alpha", "alpha", true, input.clone()).await?;
            anyhow::ensure!(
                status == StatusCode::NOT_FOUND && body["error_code"] == "unknown_space"
            );
            let error = mcp(&client, "alpha", input)
                .await
                .err()
                .ok_or_else(|| anyhow::anyhow!("missing space succeeded"))?;
            anyhow::ensure!(public_mcp_error(error)?["error_code"] == "unknown_space");
        }
    }
    Ok(())
}

#[tokio::test]
async fn route_space_and_raw_scope_are_checked_before_lookup() -> anyhow::Result<()> {
    for backend in support::BACKENDS {
        let client = support::storage(backend).await?;
        for (route, raw) in [("beta", true), ("alpha", false)] {
            let (status, body) = http(
                &client,
                route,
                "alpha",
                raw,
                json!({"query":"private report","use_vector":false}),
            )
            .await?;
            anyhow::ensure!(
                status == StatusCode::FORBIDDEN && body["error_code"] == "forbidden_space"
            );
            anyhow::ensure!(body["retryable"] == false);
        }
    }
    Ok(())
}

async fn configure(client: &StorageClient, space: &str, dimensions: u32) -> anyhow::Result<()> {
    let mut base = BaseFields::new(
        RecordId::new_v4(TableName::SpaceConfig),
        space.into(),
        "qualification".into(),
    );
    base.subjects_state = SubjectsState::Complete;
    let mut config = serde_json::to_value(base)?;
    config["embedding_provider"] = json!("fastembed");
    config["embedding_model"] = json!(format!("unused-{space}"));
    config["embedding_dims"] = json!(dimensions);
    config["purpose"] = json!(format!("Separate purpose for {space}"));
    config["skill"] = json!(format!("Separate extraction guidance for {space}"));
    config["processing"] = Value::Null;
    SpaceConfigRepository::new(client.clone())
        .create(serde_json::from_value::<SpaceConfig>(config)?)
        .await?;
    Ok(())
}

#[tokio::test]
async fn identical_external_identity_has_independent_coverage_in_each_selected_space(
) -> anyhow::Result<()> {
    for backend in support::BACKENDS {
        let client = support::storage(backend).await?;
        configure(&client, "alpha", 64).await?;
        configure(&client, "beta", 128).await?;
        let mut base = BaseFields::new(
            RecordId::new_v4(TableName::Source),
            "alpha".into(),
            "qualification".into(),
        );
        base.subjects_state = SubjectsState::Complete;
        let mut source = Source::new(
            base,
            SourceType::Manual,
            support::digest("Household private revision."),
        );
        source.external_id = Some("shared-key".into());
        source.set_manual_doc_template(&ManualDocTemplate {
            doc_id: RecordId::new_v4(TableName::Doc).to_string(),
            title: "Private household source".into(),
            text: "Household private revision.".into(),
            sidecar_text: None,
            media_attachments: vec![],
            rendition_key: "private-household".into(),
            metadata: json!({}),
        })?;
        SourcePublicationRepository::new(client.clone())
            .accept(AcceptSourceRevision {
                source,
                revision_id: "r1".into(),
                expected: SourcePrecondition::Absent,
                idempotency_key: "independent-space-coverage".into(),
                request_fingerprint: "independent-coverage-v1".into(),
                reactivate: false,
                recreate_after_erasure: false,
                authorized: None,
                authorized_document: None,
            })
            .await?;
        for (space, state) in [("alpha", "pending"), ("beta", "missing")] {
            let input = json!({"query":"original private query","limit":3,"use_vector":false,"source_revision_constraints":{"shared-key":"r1"}});
            let (status, body) = http(&client, space, space, true, input.clone()).await?;
            anyhow::ensure!(status == StatusCode::OK);
            anyhow::ensure!(
                body["coverage"].is_object() && body["coverage"]["shared-key"]["state"] == state
            );
            anyhow::ensure!(body["results"] == json!([]) && body["partial"] == true);
            anyhow::ensure!(body["coverage"]["shared-key"]["requested_revision"] == "r1");
            anyhow::ensure!(body["coverage"]["shared-key"]["published_revision"].is_null());
            anyhow::ensure!(
                support::mcp_success_data(mcp(&client, space, input).await?)? == body,
                "HTTP/MCP coverage disagrees for same selected space"
            );
            if space == "beta" {
                anyhow::ensure!(body["coverage"]["shared-key"]["desired_revision"].is_null());
            }
        }
    }
    Ok(())
}
