//! Independent public inventory oracle; pagination is over authorized records.
//! Fixed non-bypass principals test ABAC here. Signed transport is a separate target.

#[path = "../../recall_preview/crash_support.rs"]
mod crash_support;
#[path = "../../recall_preview/support.rs"]
mod support;

use axum::{
    body::{to_bytes, Body},
    http::{Request, StatusCode},
    Extension, Router,
};
use elephant_api::routes::public_sources::{public_source_routes, PublicSourcesState};
use elephant_api::{auth::Principal, authz::AuthzService, runtime::RuntimeManager};
use elephant_storage::source_publication::{
    AcceptSourceRevision, SourceMutation, SourcePublicationRepository,
};
use elephant_storage::{
    Repository, SourceRepository, SpaceConfigRepository, StorageBackendKind, StorageClient,
    StorageConfig,
};
use elephant_types::publication::{OperationState, SourcePrecondition};
use elephant_types::records::base::BaseFields;
use elephant_types::records::base::SubjectsState;
use elephant_types::records::{ManualDocTemplate, Source, SourceType, SpaceConfig};
use elephant_types::{RecordId, TableName};
use serde_json::{json, Value};
use std::{collections::BTreeSet, sync::Arc};
use tower::ServiceExt;

async fn storage(
    backend: support::Backend,
    path: &std::path::Path,
) -> anyhow::Result<StorageClient> {
    let embedded = matches!(backend, support::Backend::ElephantDb);
    let client = StorageClient::new_with_durability(
        StorageConfig {
            backend: if embedded {
                StorageBackendKind::ElephantDb
            } else {
                StorageBackendKind::SurrealDb
            },
            url: "mem://".into(),
            namespace: format!("inventory_{}", uuid::Uuid::new_v4().simple()),
            database: "qualification".into(),
            ..StorageConfig::default()
        },
        embedded.then(|| path.join("live.snapshot")),
        Some(path.join("journal")),
    )
    .await?;
    if !embedded {
        elephant_migrations::apply_schema(&client)
            .await
            .map_err(anyhow::Error::msg)?;
    }
    for (space, dims) in [("alpha", 64), ("beta", 128)] {
        let mut row = serde_json::to_value(BaseFields::new(
            RecordId::new_v4(TableName::SpaceConfig),
            space.into(),
            "qualification".into(),
        ))?;
        row["embedding_provider"] = json!(format!("fixture-{space}"));
        row["embedding_model"] = json!("no-dispatch");
        row["embedding_dims"] = json!(dims);
        row["processing"] = Value::Null;
        SpaceConfigRepository::new(client.clone())
            .create(serde_json::from_value::<SpaceConfig>(row)?)
            .await?;
    }
    Ok(client)
}

fn principal(space: &str, subject: &str, level: i32) -> Principal {
    Principal {
        subject: subject.into(),
        space_id: Some(space.into()),
        scopes: vec!["read:source".into(), "write:source".into()],
        security_level: level,
        ..Principal::default()
    }
}

fn state(client: &StorageClient) -> Arc<PublicSourcesState> {
    Arc::new(PublicSourcesState::new(
        client.clone(),
        Arc::new(AuthzService::new()),
        Arc::new(RuntimeManager::new_preconfigured(client.clone())),
    ))
}

fn router(state: Arc<PublicSourcesState>, reader: Principal) -> Router {
    public_source_routes()
        .with_state(state)
        .layer(Extension(reader))
}

async fn get(app: Router, path: &str) -> anyhow::Result<(StatusCode, Value)> {
    let response = app
        .oneshot(Request::builder().uri(path).body(Body::empty())?)
        .await?;
    let status = response.status();
    Ok((
        status,
        serde_json::from_slice(&to_bytes(response.into_body(), 262_144).await?)?,
    ))
}

async fn accept(
    client: &StorageClient,
    space: &str,
    ordinal: u128,
    level: i32,
) -> anyhow::Result<Source> {
    // Stable fixture identities; no assumption about opaque physical head order.
    let mut base = BaseFields::new(
        RecordId::new(
            TableName::Source,
            uuid::Uuid::from_u128(ordinal + if space == "beta" { 1 << 64 } else { 0 }),
        ),
        space.into(),
        "qualification".into(),
    );
    base.security.level = level;
    base.subjects_state = SubjectsState::Complete;
    let text = format!("Private inventory fixture {ordinal} in {space}.");
    let mut source = Source::new(base, SourceType::Manual, support::digest(&text));
    source.external_id = Some(format!("source-{ordinal}"));
    source.set_manual_doc_template(&ManualDocTemplate {
        doc_id: RecordId::new_v4(TableName::Doc).to_string(),
        title: format!("Inventory {ordinal}"),
        text,
        sidecar_text: None,
        media_attachments: vec![],
        rendition_key: format!("inventory-{ordinal}"),
        metadata: json!({}),
    })?;
    SourcePublicationRepository::new(client.clone())
        .accept(AcceptSourceRevision {
            source: source.clone(),
            revision_id: "r1".into(),
            expected: SourcePrecondition::Absent,
            idempotency_key: format!("inventory-{ordinal}"),
            request_fingerprint: format!("inventory-{space}-{ordinal}-{level}"),
            reactivate: false,
            recreate_after_erasure: false,
            authorized: None,
            authorized_document: None,
        })
        .await?;
    Ok(source)
}

async fn mutation(
    client: &StorageClient,
    source: &Source,
    action: &str,
) -> anyhow::Result<SourceMutation> {
    let repository = SourcePublicationRepository::new(client.clone());
    let space = &source.base.space_id;
    let authorized = repository
        .authorization_snapshot(space, &source.base.id)
        .await?
        .ok_or_else(|| anyhow::anyhow!("authorization snapshot absent"))?;
    let actor = principal(space, "qualification-operator", 9);
    let authz = AuthzService::new();
    authz
        .authorize_record_read(&actor, &authorized.envelope, "read:source")
        .map_err(|error| anyhow::anyhow!(error.message))?;
    authz
        .authorize_record_write(&actor, &authorized.envelope, "write:source")
        .map_err(|error| anyhow::anyhow!(error.message))?;
    let head = repository
        .head(space, &source.base.id)
        .await?
        .ok_or_else(|| anyhow::anyhow!("head absent"))?;
    Ok(SourceMutation {
        authorized_document: None,
        space_id: space.clone(),
        source_id: source.base.id.clone(),
        expected: SourcePrecondition::Generation(head.generation),
        idempotency_key: format!("{action}-{}", source.base.id),
        request_fingerprint: format!("{action}-{}", source.base.id),
        actor: actor.subject,
        authorized,
    })
}

#[tokio::test]
async fn wholly_unauthorized_inventory_is_empty_without_hidden_count_continuations(
) -> anyhow::Result<()> {
    for backend in support::BACKENDS {
        let directory = crash_support::CrashDirectory::new()?;
        let client = storage(backend, &directory.0).await?;
        for id in 1..=4 {
            accept(&client, "alpha", id, 9).await?;
        }
        let shared = state(&client);
        let denied = router(shared.clone(), principal("alpha", "reader", 0));
        let (status, page) = get(denied, "/v1/spaces/alpha/sources?limit=1").await?;
        anyhow::ensure!(status == StatusCode::OK);
        anyhow::ensure!(
            page == json!({"sources":[], "next_cursor":null}),
            "{backend:?}: hidden inventory disclosed rows/count/continuation: {page}"
        );
        let allowed = router(shared, principal("alpha", "operator", 9));
        let (status, page) = get(allowed, "/v1/spaces/alpha/sources?limit=100").await?;
        anyhow::ensure!(
            status == StatusCode::OK
                && page["sources"]
                    .as_array()
                    .is_some_and(|rows| rows.len() == 4),
            "negative passed only because sources were never accepted"
        );
    }
    Ok(())
}

#[tokio::test]
async fn authorized_inventory_keeps_tombstones_and_binds_cursor_to_current_caller_and_space(
) -> anyhow::Result<()> {
    for backend in support::BACKENDS {
        let directory = crash_support::CrashDirectory::new()?;
        let client = storage(backend, &directory.0).await?;
        let active = accept(&client, "alpha", 1, 0).await?;
        accept(&client, "alpha", 2, 9).await?;
        let retracted = accept(&client, "alpha", 3, 0).await?;
        accept(&client, "alpha", 4, 9).await?;
        let erased = accept(&client, "alpha", 5, 0).await?;
        let beta = accept(&client, "beta", 1, 0).await?;
        let repository = SourcePublicationRepository::new(client.clone());
        repository
            .retract(mutation(&client, &retracted, "retract").await?)
            .await?;
        let receipt = repository
            .erase_live(mutation(&client, &erased, "erase").await?)
            .await?;
        anyhow::ensure!(receipt.state == OperationState::Erased);
        anyhow::ensure!(SourceRepository::new(client.clone())
            .get_with_deleted("alpha", &erased.base.id)
            .await?
            .is_none());
        let shared = state(&client);
        let app = router(shared.clone(), principal("alpha", "reader", 0));
        let (status, first) = get(app.clone(), "/v1/spaces/alpha/sources?limit=1").await?;
        anyhow::ensure!(
            status == StatusCode::OK
                && first["sources"]
                    .as_array()
                    .is_some_and(|rows| rows.len() == 1)
        );
        let cursor = first["next_cursor"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("authorized continuation absent"))?;
        let next = format!("/v1/spaces/alpha/sources?limit=1&cursor={cursor}");
        for (other, path) in [
            (
                router(shared.clone(), principal("beta", "reader", 0)),
                format!("/v1/spaces/beta/sources?limit=1&cursor={cursor}"),
            ),
            (
                router(shared.clone(), principal("alpha", "different-reader", 0)),
                next.clone(),
            ),
            (
                router(state(&client), principal("alpha", "reader", 0)),
                next.clone(),
            ),
        ] {
            let (status, body) = get(other, &path).await?;
            anyhow::ensure!(
                status == StatusCode::BAD_REQUEST
                    && body["error_code"] == "invalid_request"
                    && body["retryable"] == false
            );
        }
        let mut path = next.clone();
        let mut rows = first["sources"]
            .as_array()
            .ok_or_else(|| anyhow::anyhow!("rows absent"))?
            .clone();
        let mut ended = false;
        for _ in 0..3 {
            let (status, page) = get(app.clone(), &path).await?;
            anyhow::ensure!(status == StatusCode::OK);
            let selected = page["sources"]
                .as_array()
                .ok_or_else(|| anyhow::anyhow!("rows absent"))?;
            anyhow::ensure!(selected.len() == 1, "hidden-only physical page exposed");
            rows.extend(selected.iter().cloned());
            match page["next_cursor"].as_str() {
                Some(cursor) => path = format!("/v1/spaces/alpha/sources?limit=1&cursor={cursor}"),
                None => {
                    ended = true;
                    break;
                }
            }
        }
        anyhow::ensure!(ended && rows.len() == 3);
        let ids: BTreeSet<_> = rows
            .iter()
            .map(|row| row["internal_source_id"].to_string())
            .collect();
        anyhow::ensure!(ids.len() == 3);
        for (source, lifecycle) in [
            (&active, "active"),
            (&retracted, "retracted"),
            (&erased, "erased"),
        ] {
            let row = rows
                .iter()
                .find(|row| row["internal_source_id"] == json!(source.base.id))
                .ok_or_else(|| anyhow::anyhow!("authorized head/tombstone absent"))?;
            anyhow::ensure!(
                row["lifecycle"] == lifecycle
                    && row["operation_id"].is_string()
                    && row["generation"].is_string()
            );
            anyhow::ensure!(row["published_revision"].is_null());
        }
        let (status, other) = get(
            router(shared.clone(), principal("beta", "reader", 0)),
            "/v1/spaces/beta/sources?limit=1",
        )
        .await?;
        anyhow::ensure!(
            status == StatusCode::OK
                && other["sources"][0]["internal_source_id"] == json!(beta.base.id)
                && other["sources"][0]["lifecycle"] == "active"
        );
        // Cursors grant continuation, never a frozen authorization snapshot.
        // Pick a remaining non-erased row without depending on opaque key order.
        let hidden_id: RecordId = serde_json::from_value(
            rows[1..]
                .iter()
                .find(|row| row["lifecycle"] != "erased")
                .ok_or_else(|| anyhow::anyhow!("mutable continuation row absent"))?
                ["internal_source_id"]
                .clone(),
        )?;
        let retained_id = rows[1..]
            .iter()
            .find(|row| row["internal_source_id"] != json!(hidden_id))
            .ok_or_else(|| anyhow::anyhow!("other continuation row absent"))?["internal_source_id"]
            .clone();
        let mut classified = SourceRepository::new(client.clone())
            .get_with_deleted("alpha", &hidden_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("remaining source absent"))?;
        classified.base.security.level = 9;
        SourceRepository::new(client.clone())
            .update("alpha", &hidden_id, classified)
            .await?;
        let (status, page) = get(app.clone(), &next).await?;
        anyhow::ensure!(
            status == StatusCode::OK
                && page["sources"]
                    .as_array()
                    .is_some_and(|rows| rows.len() == 1)
        );
        anyhow::ensure!(
            page["sources"][0]["internal_source_id"] == retained_id
                && page["next_cursor"].is_null(),
            "cursor reused old record clearance"
        );
        for query in [
            "limit=0",
            "limit=101",
            "cursor=invalid",
            "unsupported=value",
        ] {
            let (status, body) =
                get(app.clone(), &format!("/v1/spaces/alpha/sources?{query}")).await?;
            anyhow::ensure!(
                status == StatusCode::BAD_REQUEST && body["error_code"] == "invalid_request"
            );
        }
    }
    Ok(())
}
