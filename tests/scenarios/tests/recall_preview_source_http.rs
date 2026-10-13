//! Actual source HTTP decoding, byte bounds, acceptance and retry contracts.
//! No worker/provider runs; accepted/queued never counts as complete publication.

#[path = "../../recall_preview/support.rs"]
mod support;

use axum::{
    body::{to_bytes, Body},
    http::{Request, StatusCode},
    Extension, Router,
};
use elephant_api::routes::public_sources::{
    public_source_routes, PublicSourcesState, MAX_SOURCE_JSON_BYTES,
};
use elephant_api::{auth::Principal, authz::AuthzService, runtime::RuntimeManager};
use elephant_storage::source_publication::SourcePublicationRepository;
use elephant_storage::{
    Repository, SourceRepository, SpaceConfigRepository, StorageClient, WorkItemRepository,
};
use elephant_types::records::base::BaseFields;
use elephant_types::records::{SourceType, SpaceConfig};
use elephant_types::{RecordId, TableName};
use serde_json::{json, Value};
use std::{collections::BTreeSet, sync::Arc};
use tower::ServiceExt;

const SPACE: &str = "source-http-qualification";
const MAX_TEXT_BYTES: usize = 2_000_000;

async fn app(client: &StorageClient) -> anyhow::Result<Router> {
    let mut config = serde_json::to_value(BaseFields::new(
        RecordId::new_v4(TableName::SpaceConfig),
        SPACE.into(),
        "qualification".into(),
    ))?;
    config["embedding_provider"] = json!("fixture-no-dispatch");
    config["embedding_model"] = json!("unused");
    config["embedding_dims"] = json!(3);
    config["processing"] = Value::Null;
    SpaceConfigRepository::new(client.clone())
        .create(serde_json::from_value::<SpaceConfig>(config)?)
        .await?;
    let state = Arc::new(PublicSourcesState::new(
        client.clone(),
        Arc::new(AuthzService::new()),
        Arc::new(RuntimeManager::new_preconfigured(client.clone())),
    ));
    Ok(public_source_routes()
        .with_state(state)
        .layer(Extension(Principal {
            subject: "qualification-writer".into(),
            space_id: Some(SPACE.into()),
            scopes: vec!["read:source".into(), "write:source".into()],
            ..Principal::default()
        })))
}

fn input(text: &str) -> Value {
    json!({"revision":"r1","text":text,"media_type":"text/plain",
        "observed_at":"2026-10-06T20:11:00Z","subjects_state":"complete"})
}

async fn put(
    app: Router,
    external: &str,
    key: &str,
    bytes: Vec<u8>,
) -> anyhow::Result<(StatusCode, Value)> {
    let response = app
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(format!("/v1/spaces/{SPACE}/sources/{external}"))
                .header("content-type", "application/json")
                .header("idempotency-key", key)
                .body(Body::from(bytes))?,
        )
        .await?;
    let status = response.status();
    Ok((
        status,
        serde_json::from_slice(&to_bytes(response.into_body(), 262_144).await?)?,
    ))
}

async fn queued_ids(client: &StorageClient) -> anyhow::Result<BTreeSet<RecordId>> {
    Ok(WorkItemRepository::new(client.clone())
        .list(SPACE, 100, None)
        .await?
        .into_iter()
        .map(|item| item.base.id)
        .collect())
}

#[tokio::test]
async fn source_http_distinguishes_decoded_utf8_and_encoded_body_limits_without_partial_acceptance(
) -> anyhow::Result<()> {
    for backend in support::BACKENDS {
        let client = support::storage(backend).await?;
        let app = app(&client).await?;
        let repository = SourcePublicationRepository::new(client.clone());
        let ascii = "a".repeat(MAX_TEXT_BYTES + 1);
        let unicode = format!("{}a", "é".repeat(MAX_TEXT_BYTES / 2));
        let mut oversized_transport = vec![b' '; MAX_SOURCE_JSON_BYTES];
        oversized_transport.extend(serde_json::to_vec(&input("small decoded text"))?);
        for (name, bytes) in [
            ("ascii-too-large", serde_json::to_vec(&input(&ascii))?),
            ("unicode-too-large", serde_json::to_vec(&input(&unicode))?),
            ("transport-too-large", oversized_transport),
        ] {
            let (status, error) = put(app.clone(), name, name, bytes).await?;
            anyhow::ensure!(
                status == StatusCode::PAYLOAD_TOO_LARGE
                    && error["error_code"] == "payload_too_large"
                    && error["retryable"] == false,
                "{backend:?}/{name}: wrong public size outcome: {status} {error}"
            );
            anyhow::ensure!(repository
                .external_head(SPACE, SourceType::Manual, name)
                .await?
                .is_none());
            anyhow::ensure!(SourceRepository::new(client.clone())
                .list(SPACE, 1, None)
                .await?
                .is_empty());
            anyhow::ensure!(
                queued_ids(&client).await?.is_empty(),
                "rejected source enqueued work"
            );
        }
        let (status, error) = put(app.clone(), "malformed", "malformed", b"{".to_vec()).await?;
        anyhow::ensure!(
            status == StatusCode::BAD_REQUEST && error["error_code"] == "invalid_request"
        );
        anyhow::ensure!(queued_ids(&client).await?.is_empty());

        let unicode = "é".repeat(MAX_TEXT_BYTES / 2);
        let ascii = "a".repeat(MAX_TEXT_BYTES);
        // Every decoded ASCII byte uses a six-byte JSON escape. This must still
        // fit the explicitly larger transport envelope and reach acceptance.
        let escaped = format!("{{\"revision\":\"r1\",\"text\":\"{}\",\"media_type\":\"text/plain\",\"observed_at\":\"2026-10-06T20:11:00Z\",\"subjects_state\":\"complete\"}}", "\\u0061".repeat(MAX_TEXT_BYTES));
        anyhow::ensure!(escaped.len() < MAX_SOURCE_JSON_BYTES && escaped.len() > MAX_TEXT_BYTES);
        for (name, text, bytes) in [
            (
                "unicode-at-limit",
                unicode.clone(),
                serde_json::to_vec(&input(&unicode))?,
            ),
            ("escaped-at-limit", ascii, escaped.into_bytes()),
        ] {
            let before = queued_ids(&client).await?;
            let (status, receipt) = put(app.clone(), name, name, bytes).await?;
            anyhow::ensure!(
                status == StatusCode::ACCEPTED
                    && receipt["desired_revision"] == "r1"
                    && receipt["published_revision"].is_null()
            );
            let head = repository
                .external_head(SPACE, SourceType::Manual, name)
                .await?
                .ok_or_else(|| anyhow::anyhow!("accepted head missing"))?;
            let revision = repository
                .revision(SPACE, &head.source_id, "r1")
                .await?
                .ok_or_else(|| anyhow::anyhow!("accepted immutable input missing"))?;
            let template = revision
                .source
                .manual_doc_template()?
                .ok_or_else(|| anyhow::anyhow!("accepted text missing"))?;
            anyhow::ensure!(
                template.text.as_bytes() == text.as_bytes()
                    && template.text.len() == MAX_TEXT_BYTES
            );
            anyhow::ensure!(
                queued_ids(&client).await?.len() > before.len(),
                "accepted input never queued"
            );
            anyhow::ensure!(
                head.published_revision.is_none(),
                "mere HTTP acceptance claimed search coverage"
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn source_http_observation_retry_keeps_operation_and_queue_identity_but_changed_input_conflicts(
) -> anyhow::Result<()> {
    for backend in support::BACKENDS {
        let client = support::storage(backend).await?;
        let app = app(&client).await?;
        let repository = SourcePublicationRepository::new(client.clone());
        let mut request = input("The original accepted evidence stays byte exact.");
        let (status, first) = put(
            app.clone(),
            "stable-key",
            "stable-retry",
            serde_json::to_vec(&request)?,
        )
        .await?;
        anyhow::ensure!(status == StatusCode::ACCEPTED);
        let before = queued_ids(&client).await?;
        anyhow::ensure!(!before.is_empty());
        request["observed_at"] = json!("2026-10-07T20:11:00Z");
        let (status, retry) = put(
            app.clone(),
            "stable-key",
            "stable-retry",
            serde_json::to_vec(&request)?,
        )
        .await?;
        anyhow::ensure!(status == StatusCode::ACCEPTED && retry == first);
        anyhow::ensure!(queued_ids(&client).await? == before);
        let head_before = repository
            .external_head(SPACE, SourceType::Manual, "stable-key")
            .await?;
        request["text"] = json!("Changed bytes under the same idempotency key.");
        let (status, error) = put(
            app,
            "stable-key",
            "stable-retry",
            serde_json::to_vec(&request)?,
        )
        .await?;
        anyhow::ensure!(
            status == StatusCode::CONFLICT
                && error["error_code"] == "conflict_generation"
                && error["retryable"] == false
        );
        anyhow::ensure!(
            repository
                .external_head(SPACE, SourceType::Manual, "stable-key")
                .await?
                == head_before
        );
        anyhow::ensure!(queued_ids(&client).await? == before);
    }
    Ok(())
}
