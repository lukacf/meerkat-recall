//! Actual pipeline publications read through the mounted public HTTP route.
//! Fixed local provider transport establishes plumbing, not model qualification.
#![allow(clippy::too_many_lines, clippy::large_futures)]

use std::sync::Arc;

use anyhow::Context;
use axum::{
    body::{to_bytes, Body},
    http::{Request, StatusCode},
    Extension,
};
use elephant_api::{
    routes::docs::{doc_routes, DocsState},
    AuthzService, Principal,
};
use elephant_pipeline::extraction::AssertionValidator;
use elephant_storage::{
    source_intake::{IntakeControl, PreparedSourceIntake},
    source_publication::{SourceMutation, SourcePublicationRepository},
    DocRepository, DocRevisionRepository, Repository, SourceRepository, StorageClient,
};
use elephant_types::publication::{OperationState, SourcePrecondition};
use elephant_types::records::{DocRevision, RevisionStatus};
use elephant_types::{RecordId, TableName};
use tower::ServiceExt;

use crate::grounded_source_fixture::{
    fixture_published_revision, EmploymentExtractor, GroundedSourceFixture,
};

fn reader(space: &str) -> Principal {
    Principal {
        subject: "revision-inspector".into(),
        space_id: Some(space.into()),
        scopes: vec!["read:doc".into(), "read:doc:raw".into()],
        // Default level zero intentionally cannot read a newly classified parent.
        ..Principal::default()
    }
}

async fn request(
    storage: &StorageClient,
    principal: Principal,
    doc: &RecordId,
    revision: &RecordId,
) -> anyhow::Result<(StatusCode, serde_json::Value)> {
    let state = Arc::new(DocsState::new(storage, Arc::new(AuthzService::new())));
    let response = doc_routes()
        .with_state(state)
        .layer(Extension(principal))
        .oneshot(
            Request::builder()
                .uri(format!("/v1/docs/{doc}/revisions/{revision}"))
                .body(Body::empty())?,
        )
        .await?;
    let status = response.status();
    let bytes = to_bytes(response.into_body(), 12_065_536).await?;
    Ok((status, serde_json::from_slice(&bytes)?))
}

async fn assert_revision(
    storage: &StorageClient,
    space: &str,
    revision: &DocRevision,
) -> anyhow::Result<()> {
    let (status, body) =
        request(storage, reader(space), revision.doc.id(), &revision.base.id).await?;
    anyhow::ensure!(
        status == StatusCode::OK,
        "revision route refused: {status} {body}"
    );
    let actual: DocRevision = serde_json::from_value(body)?;
    anyhow::ensure!(actual == *revision, "public DTO must be full and lossless");
    Ok(())
}

async fn actual_publication_reads(storage: StorageClient) -> anyhow::Result<()> {
    elephant_migrations::apply_schema(&storage)
        .await
        .map_err(anyhow::Error::msg)?;
    let fixture = GroundedSourceFixture::new(&storage, "http-revision-oracle").await?;
    let extractor = EmploymentExtractor::new();
    let first = Box::pin(fixture.publish_text(
        &storage,
        "Original",
        "Alice works at Acme Corp.  \r\n",
        &extractor,
    ))
    .await?;
    let revision_id = fixture_published_revision(&storage, &first).await?;
    let revisions = DocRevisionRepository::new(storage.clone());
    let docs = DocRepository::new(storage.clone());
    let sources = SourceRepository::new(storage.clone());
    let original = revisions
        .get(&fixture.space, &revision_id)
        .await?
        .context("original revision missing")?;
    anyhow::ensure!(original.text == "Alice works at Acme Corp.\n");
    anyhow::ensure!(original.status == RevisionStatus::Active);
    assert_revision(&storage, &fixture.space, &original).await?;

    let mut metadata_only = reader(&fixture.space);
    metadata_only.scopes = vec!["read:doc".into()];
    let (status, body) = request(&storage, metadata_only, original.doc.id(), &revision_id).await?;
    anyhow::ensure!(status == StatusCode::FORBIDDEN && body["error_code"] == "forbidden_space");
    let (status, _) = request(
        &storage,
        reader(&fixture.space),
        &RecordId::new_v4(TableName::Doc),
        &revision_id,
    )
    .await?;
    anyhow::ensure!(status == StatusCode::NOT_FOUND);

    // A changed hash can be internally consistent and still lack the original
    // completed-render proof. Restore exact originals after each negative.
    for field in ["text", "renderer", "manifest", "policy"] {
        let mut changed = original.clone();
        match field {
            "text" => {
                changed.text = "Unpublished forged rendering.\n".into();
                changed.text_hash = AssertionValidator::hash_quote(&changed.text);
            }
            "renderer" => changed.renderer.version.push_str("-changed"),
            "manifest" => changed.manifest = Some(Default::default()),
            _ => changed.base.security.level = 7,
        }
        revisions
            .update(&fixture.space, &revision_id, changed)
            .await?;
        let (status, _) = request(
            &storage,
            reader(&fixture.space),
            original.doc.id(),
            &revision_id,
        )
        .await?;
        anyhow::ensure!(
            matches!(status, StatusCode::NOT_FOUND | StatusCode::FORBIDDEN),
            "changed {field} escaped: {status}"
        );
        revisions
            .update(&fixture.space, &revision_id, original.clone())
            .await?;
        assert_revision(&storage, &fixture.space, &original).await?;
    }
    let original_doc = docs
        .get(&fixture.space, original.doc.id())
        .await?
        .context("original doc missing")?;
    let mut restricted = original_doc.clone();
    restricted.base.security.level = 7;
    docs.update(&fixture.space, original.doc.id(), restricted)
        .await?;
    let (status, _) = request(
        &storage,
        reader(&fixture.space),
        original.doc.id(),
        &revision_id,
    )
    .await?;
    anyhow::ensure!(status == StatusCode::FORBIDDEN);
    docs.update(&fixture.space, original.doc.id(), original_doc)
        .await?;

    let original_source = sources
        .get(&fixture.space, &first.source.base.id)
        .await?
        .context("source missing")?;
    let mut restricted = original_source.clone();
    restricted.base.security.level = 7;
    sources
        .update(&fixture.space, &first.source.base.id, restricted)
        .await?;
    let (status, _) = request(
        &storage,
        reader(&fixture.space),
        original.doc.id(),
        &revision_id,
    )
    .await?;
    anyhow::ensure!(
        status == StatusCode::NOT_FOUND,
        "current parent must remain authorized"
    );
    sources
        .update(&fixture.space, &first.source.base.id, original_source)
        .await?;
    assert_revision(&storage, &fixture.space, &original).await?;

    let next_text = "Alice works at Acme Corp.  \r\nNew original rendering context.\r\n";
    let mut next_source = first.source.clone();
    next_source.hash = AssertionValidator::hash_quote(next_text);
    let mut template = next_source
        .manual_doc_template()?
        .context("original raw template missing")?;
    template.doc_id = RecordId::new_v4(TableName::Doc).to_string();
    template.text = next_text.into();
    next_source.set_manual_doc_template(&template)?;
    let prepared = Box::pin(PreparedSourceIntake::prepare(
        &storage,
        next_source,
        IntakeControl {
            revision_id: Some("http-revision-R2".into()),
            idempotency_key: Some("http-revision-R2-original".into()),
            expected_generation: Some(first.publication.generation.clone()),
        },
    ))
    .await?;
    let second = Box::pin(prepared.accept(&storage)).await?;
    // Actual desired R2 is pending, so the original published R1 stays readable.
    assert_revision(&storage, &fixture.space, &original).await?;
    elephant_pipeline::source_queue::enqueue_accepted_source(
        &storage,
        &second.source,
        &second.publication,
        "http-revision-oracle",
    )
    .await?;
    Box::pin(fixture.drive(&storage, &extractor)).await?;
    let publications = SourcePublicationRepository::new(storage.clone());
    let completion = publications
        .operation(&fixture.space, &second.operation.operation_id)
        .await?
        .context("second operation missing")?;
    anyhow::ensure!(completion.state == OperationState::Completed);
    let (status, _) = request(
        &storage,
        reader(&fixture.space),
        original.doc.id(),
        &revision_id,
    )
    .await?;
    anyhow::ensure!(
        status == StatusCode::NOT_FOUND,
        "superseded R1 must not borrow R2 completion"
    );
    let second_revision_id = fixture_published_revision(&storage, &second).await?;
    let second_revision = revisions
        .get(&fixture.space, &second_revision_id)
        .await?
        .context("R2 revision missing")?;
    assert_revision(&storage, &fixture.space, &second_revision).await?;

    let retracted = publications
        .retract(SourceMutation {
            space_id: fixture.space.clone(),
            source_id: first.source.base.id.clone(),
            expected: SourcePrecondition::Generation(second.publication.generation.clone()),
            idempotency_key: "http-revision-retract".into(),
            request_fingerprint: "http-revision-retract".into(),
            actor: "fixture".into(),
            authorized: publications
                .authorization_snapshot(&fixture.space, &first.source.base.id)
                .await?
                .context("current authority missing")?,
            authorized_document: None,
        })
        .await?;
    let (status, _) = request(
        &storage,
        reader(&fixture.space),
        second_revision.doc.id(),
        &second_revision_id,
    )
    .await?;
    anyhow::ensure!(status == StatusCode::NOT_FOUND);
    publications
        .erase_live(SourceMutation {
            space_id: fixture.space.clone(),
            source_id: first.source.base.id.clone(),
            expected: SourcePrecondition::Generation(retracted.source_generation),
            idempotency_key: "http-revision-erase".into(),
            request_fingerprint: "http-revision-erase".into(),
            actor: "fixture".into(),
            authorized: publications
                .authorization_snapshot(&fixture.space, &first.source.base.id)
                .await?
                .context("retracted authority missing")?,
            authorized_document: None,
        })
        .await?;
    let (status, _) = request(
        &storage,
        reader(&fixture.space),
        second_revision.doc.id(),
        &second_revision_id,
    )
    .await?;
    anyhow::ensure!(status == StatusCode::NOT_FOUND);
    Ok(())
}

#[tokio::test]
async fn embedded_http_revision_tracks_actual_original_publication() -> anyhow::Result<()> {
    Box::pin(actual_publication_reads(
        StorageClient::new_embedded().await?,
    ))
    .await
}

#[tokio::test]
async fn native_http_revision_tracks_actual_original_publication() -> anyhow::Result<()> {
    Box::pin(actual_publication_reads(StorageClient::new_memory().await?)).await
}
