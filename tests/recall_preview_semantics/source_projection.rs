//! Published Source reads use actual current and immutable authorization.
//! Controlled embedding transport is fixture plumbing, not relevance evidence.

use super::{index_fixture as fixture, spy};
use anyhow::Context;
use axum::{
    body::{to_bytes, Body},
    http::{Request, StatusCode},
    Extension,
};
use elephant_api::{
    routes::sources::{source_routes, SourcesState},
    AuthzService, Principal,
};
use elephant_mcp::handlers::{SearchSourcesHandler, ToolContext, ToolHandler};
use elephant_storage::source_publication::{AcceptSourceRevision, SourcePublicationRepository};
use elephant_storage::source_publication_manifest::SourcePublicationManifestRepository;
use elephant_storage::{Repository, SourceRepository, SpaceConfigRepository, StorageClient};
use elephant_types::publication::{PublicationFence, SourcePrecondition};
use elephant_types::records::ingest::IngestMode;
use elephant_types::records::SourceType;
use serde_json::json;
use std::{collections::BTreeMap, sync::Arc};
use tower::ServiceExt;

fn reader(storage: &StorageClient, space: &str, policies: &[&str]) -> ToolContext {
    let mut context = fixture::reader(storage, space);
    context.principal.scopes.insert("read:source".into());
    // Default permits the existing search preflight; only the named explicit
    // policy grants authorize the actual source envelopes.
    context.principal.clearances.retain(|clearance| {
        clearance.policy_id == "default" || policies.contains(&clearance.policy_id.as_str())
    });
    context
}

async fn search(
    storage: &StorageClient,
    space: &str,
    policies: &[&str],
) -> anyhow::Result<serde_json::Value> {
    let result = Box::pin(SearchSourcesHandler::new(storage.clone()).execute(
        &reader(storage, space, policies),
        json!({"query":"source-projection-contract"}),
    ))
    .await?;
    anyhow::ensure!(result["ok"] == true);
    Ok(result)
}

async fn http_exchange(
    storage: &StorageClient,
    space: &str,
    policies: &[&str],
    source: Option<&elephant_types::RecordId>,
) -> anyhow::Result<(StatusCode, serde_json::Value)> {
    let context = reader(storage, space, policies);
    let principal = Principal {
        subject: "source-projection-reader".into(),
        space_id: Some(space.into()),
        scopes: vec!["read:source".into()],
        clearances: context.principal.clearances,
        ..Principal::default()
    };
    let app = source_routes()
        .with_state(Arc::new(SourcesState::new(
            storage.clone(),
            Arc::new(AuthzService::new()),
        )))
        .layer(Extension(principal));
    let (method, path, body) = source.map_or_else(
        || {
            (
                "POST",
                "/v1/sources/search".to_owned(),
                Body::from(r#"{"query":"source-projection-contract"}"#),
            )
        },
        |id| ("GET", format!("/v1/sources/{id}"), Body::empty()),
    );
    let response = Box::pin(
        app.oneshot(
            Request::builder()
                .method(method)
                .uri(path)
                .header("content-type", "application/json")
                .body(body)?,
        ),
    )
    .await?;
    let status = response.status();
    let raw = to_bytes(response.into_body(), 65_536).await?;
    Ok((status, serde_json::from_slice(&raw)?))
}

async fn http_visible(
    storage: &StorageClient,
    space: &str,
    policies: &[&str],
    source: &elephant_types::RecordId,
) -> anyhow::Result<()> {
    let (status, search) = http_exchange(storage, space, policies, None).await?;
    anyhow::ensure!(status == StatusCode::OK);
    let items = search["results"].as_array().context("HTTP source items")?;
    anyhow::ensure!(items.len() == 1 && items[0]["id"] == source.to_string());
    anyhow::ensure!(items[0]["security"]["policy_id"] == "index-a");
    anyhow::ensure!(items[0]["current_ingest_mode"] == "retrieval");
    anyhow::ensure!(items[0]["lifecycle_status"] == "retrieval_ready");
    anyhow::ensure!(items[0].get("latest_ingest_decision") == Some(&serde_json::Value::Null));
    anyhow::ensure!(search["total"] == 1);
    let (status, direct) = http_exchange(storage, space, policies, Some(source)).await?;
    anyhow::ensure!(status == StatusCode::OK);
    anyhow::ensure!(direct["id"] == source.to_string());
    anyhow::ensure!(direct["security"]["policy_id"] == "index-a");
    anyhow::ensure!(direct["current_ingest_mode"] == "retrieval");
    anyhow::ensure!(direct["lifecycle_status"] == "retrieval_ready");
    anyhow::ensure!(direct.get("latest_ingest_decision") == Some(&serde_json::Value::Null));
    Ok(())
}

#[tokio::test]
async fn published_source_search_authorizes_current_and_immutable_envelopes() -> anyhow::Result<()>
{
    Box::pin(run()).await
}

#[allow(clippy::too_many_lines)]
async fn run() -> anyhow::Result<()> {
    const EXTERNAL: &str = "source-projection-contract";
    const TEXT: &str = "The archived source describes a signed household agreement.";
    for backend in spy::BACKENDS {
        let storage = Box::pin(spy::storage(backend)).await?;
        let configurations = SpaceConfigRepository::new(storage.clone());
        for (index, space) in spy::SPACES.into_iter().enumerate() {
            let endpoint = spy::Spy::start().await?;
            let dimensions = 8 + index * 4;
            Box::pin(spy::configure(
                &configurations,
                space,
                &endpoint.endpoint,
                dimensions,
            ))
            .await?;
            let mut vector = vec![0.0; dimensions];
            vector[0] = 1.0;
            endpoint.declared_vectors(BTreeMap::from([(TEXT.into(), vector)]))?;
            Box::pin(fixture::publish(
                &storage,
                space,
                EXTERNAL,
                "original-r1",
                TEXT,
                fixture::security("index-a", 0),
                &[],
            ))
            .await?;
            let publications = SourcePublicationRepository::new(storage.clone());
            let head = publications
                .external_head(space, SourceType::Manual, EXTERNAL)
                .await?
                .context("actual published source head")?;
            anyhow::ensure!(head.published_revision.as_deref() == Some("original-r1"));
            let original_publication = PublicationFence {
                source_id: head.source_id.clone(),
                generation: head.generation.clone(),
                revision_id: "original-r1".into(),
                operation_id: head.operation_id.clone(),
            };
            let manifests = SourcePublicationManifestRepository::new(storage.clone());
            let completion = manifests
                .completion(space, &original_publication)
                .await?
                .context("actual original Retrieval completion")?;
            anyhow::ensure!(completion.publication == original_publication);
            let progress = completion
                .published_progress
                .as_ref()
                .context("original progress proof missing")?;
            anyhow::ensure!(progress.mode == IngestMode::Retrieval);
            // This direct fixture has no triage ancestor. A missing certificate
            // cannot stand in for the explicit certified absence of a decision.
            anyhow::ensure!(progress.latest_ingest_decision.is_none());
            anyhow::ensure!(progress.ingest_decision_fingerprint.is_none());
            let original = publications
                .revision(space, &head.source_id, "original-r1")
                .await?
                .context("original immutable source")?;
            let current = SourceRepository::new(storage.clone())
                .get(space, &head.source_id)
                .await?
                .context("current source envelope")?;
            anyhow::ensure!(current.base.updated_at != original.source.base.updated_at);
            let visible = search(&storage, space, &["index-a"]).await?;
            let items = visible["data"]["items"]
                .as_array()
                .context("source items")?;
            anyhow::ensure!(items.len() == 1 && items[0]["id"] == head.source_id.to_string());
            anyhow::ensure!(items[0]["security"]["policy_id"] == "index-a");
            anyhow::ensure!(items[0]["current_ingest_mode"] == "retrieval");
            anyhow::ensure!(items[0]["lifecycle_status"] == "retrieval_ready");
            anyhow::ensure!(
                items[0].get("latest_ingest_decision") == Some(&serde_json::Value::Null)
            );
            http_visible(&storage, space, &["index-a"], &head.source_id).await?;

            // Accept a successor with a different policy without publishing it.
            // The old published source stays visible only to a principal with
            // access to BOTH the current envelope and that immutable envelope.
            let authorized = publications
                .authorization_snapshot(space, &head.source_id)
                .await?
                .context("actual current source authorization")?;
            let mut successor = current;
            successor.base.security = fixture::security("index-b", 0);
            let accepted = publications
                .accept(AcceptSourceRevision {
                    source: successor,
                    revision_id: "pending-r2".into(),
                    expected: SourcePrecondition::Generation(head.generation),
                    idempotency_key: "source-projection-pending-r2".into(),
                    request_fingerprint: fixture::digest("source-projection-pending-r2"),
                    reactivate: false,
                    recreate_after_erasure: false,
                    authorized: Some(authorized),
                    authorized_document: None,
                })
                .await?;
            anyhow::ensure!(accepted.published_revision.as_deref() == Some("original-r1"));
            for (policies, direct_status) in [
                (&["index-a"][..], StatusCode::FORBIDDEN),
                (&["index-b"][..], StatusCode::NOT_FOUND),
                (&[][..], StatusCode::FORBIDDEN),
            ] {
                let hidden = search(&storage, space, policies).await?;
                anyhow::ensure!(hidden["data"]["items"]
                    .as_array()
                    .context("denied items")?
                    .is_empty());
                anyhow::ensure!(hidden["data"]["total"] == 0);
                let (status, hidden) = http_exchange(&storage, space, policies, None).await?;
                anyhow::ensure!(status == StatusCode::OK);
                anyhow::ensure!(hidden["results"]
                    .as_array()
                    .context("denied HTTP source items")?
                    .is_empty());
                anyhow::ensure!(hidden["total"] == 0);
                let (status, denied) =
                    http_exchange(&storage, space, policies, Some(&head.source_id)).await?;
                anyhow::ensure!(status == direct_status);
                anyhow::ensure!(denied.get("id").is_none());
                anyhow::ensure!(denied.get("security").is_none());
            }
            let visible = search(&storage, space, &["index-a", "index-b"]).await?;
            let items = visible["data"]["items"]
                .as_array()
                .context("published items")?;
            anyhow::ensure!(items.len() == 1 && items[0]["id"] == head.source_id.to_string());
            anyhow::ensure!(items[0]["security"]["policy_id"] == "index-a");
            anyhow::ensure!(items[0]["current_ingest_mode"] == "retrieval");
            anyhow::ensure!(items[0]["lifecycle_status"] == "retrieval_ready");
            anyhow::ensure!(
                items[0].get("latest_ingest_decision") == Some(&serde_json::Value::Null)
            );
            anyhow::ensure!(visible["data"]["total"] == 1);
            http_visible(&storage, space, &["index-a", "index-b"], &head.source_id).await?;
            anyhow::ensure!(
                manifests.completion(space, &original_publication).await? == Some(completion),
                "pending successor changed the original completed progress proof"
            );
        }
    }
    Ok(())
}
