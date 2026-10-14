//! Full graph/storage publication with deterministic verified interpretation.
//! Index vectors are storage fixtures. Public reads explicitly use lexical mode;
//! no assertion here is evidence of real model quality or vector relevance.

#[path = "../../recall_preview/graph_support.rs"]
mod graph_support;
#[path = "../../recall_preview/publication_support.rs"]
mod publication_support;
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
use elephant_storage::source_publication::SourcePublicationRepository;
use elephant_storage::source_publication_manifest::SourcePublicationManifestRepository;
use elephant_storage::{
    AssertionRepository, EntityRepository, RelRepository, Repository, WorkArtifactRepository,
    WorkItemRepository,
};
use elephant_types::publication::{OperationState, SourcePrecondition};
use graph_support::GraphFixture;
use serde_json::{json, Value};
use std::{collections::HashSet, sync::Arc};
use tower::ServiceExt;

const SPACE: &str = "qualification-graph";

async fn no_graph(fixture: &GraphFixture) -> anyhow::Result<()> {
    let client = &fixture.publication.client;
    anyhow::ensure!(EntityRepository::new(client.clone())
        .list(SPACE, 100, None)
        .await?
        .is_empty());
    anyhow::ensure!(AssertionRepository::new(client.clone())
        .list(SPACE, 100, None)
        .await?
        .is_empty());
    anyhow::ensure!(RelRepository::new(client.clone())
        .list(SPACE, 100, None)
        .await?
        .is_empty());
    let operation = SourcePublicationRepository::new(client.clone())
        .operation(SPACE, &fixture.publication.accepted.operation_id)
        .await?
        .ok_or_else(|| anyhow::anyhow!("source operation absent"))?;
    anyhow::ensure!(
        operation.state != OperationState::Completed && operation.published_revision.is_none()
    );
    Ok(())
}

#[tokio::test]
async fn complete_graph_and_receipt_wait_for_real_index_rows_and_embedding_work(
) -> anyhow::Result<()> {
    for backend in support::BACKENDS {
        let fixture = GraphFixture::new(support::storage(backend).await?, SPACE).await?;
        let declared = &fixture.publication.manifest.embedding_work_item_ids;
        anyhow::ensure!(declared.len() == 1);
        anyhow::ensure!(
            WorkItemRepository::new(fixture.publication.client.clone())
                .get(SPACE, &declared[0])
                .await?
                .is_none(),
            "negative control unexpectedly has its declared embedding predecessor"
        );
        let missing = fixture
            .publication
            .finalize()
            .await
            .err()
            .ok_or_else(|| anyhow::anyhow!("missing embeddings falsely completed source"))?;
        let reason = missing.to_string();
        // The original embedding predecessor is deliberately absent. Native
        // checks report the common predecessor category before row coverage;
        // embedded checks name the missing work item. The positive below adds
        // ONLY actual indexed vectors and their original completed work, so a
        // different missing predecessor cannot make this case pass.
        anyhow::ensure!(
            reason.contains("embedding")
                || reason.contains("source_publication_incomplete:predecessor_missing")
                || reason.contains("source_publication_incomplete:predecessor_work_missing"),
            "graph negative control failed at unrelated prerequisite: {missing}"
        );
        no_graph(&fixture).await?;
        fixture.index_fixture_vectors().await?;
        let receipt = fixture.publication.finalize().await?;
        let client = &fixture.publication.client;
        let sources = SourcePublicationRepository::new(client.clone());
        let operation = sources
            .operation(SPACE, &fixture.publication.accepted.operation_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("completed operation absent"))?;
        anyhow::ensure!(
            operation.state == OperationState::Completed
                && operation.published_revision.as_deref() == Some("oriole-revision-1")
        );
        let entities = EntityRepository::new(client.clone())
            .list(SPACE, 100, None)
            .await?;
        let assertions = AssertionRepository::new(client.clone())
            .list(SPACE, 100, None)
            .await?;
        let relations = RelRepository::new(client.clone())
            .list(SPACE, 100, None)
            .await?;
        anyhow::ensure!(
            entities.len() == 2 && assertions.len() == 1 && relations.len() == 1,
            "source completion omitted canonical graph effects"
        );
        for base in entities
            .iter()
            .map(|row| &row.base)
            .chain(assertions.iter().map(|row| &row.base))
            .chain(relations.iter().map(|row| &row.base))
        {
            anyhow::ensure!(base.security == fixture.lineage.source.base.security);
            anyhow::ensure!(base.subjects_state == fixture.lineage.source.base.subjects_state);
            anyhow::ensure!(fixture
                .lineage
                .source
                .base
                .subjects
                .iter()
                .all(|subject| base.subjects.contains(subject)));
            anyhow::ensure!(receipt.effect_ids.contains(&base.id));
            let support = sources.support_for_record(SPACE, &base.id).await?;
            anyhow::ensure!(
                support.len() == 1
                    && support[0].source_id == fixture.lineage.source.base.id
                    && support[0].source_revision == "oriole-revision-1"
            );
        }
        let evidence = &assertions[0].evidence;
        anyhow::ensure!(
            evidence.revision.id() == &fixture.lineage.revision.base.id
                && evidence.node.as_ref().map(|node| node.id())
                    == Some(&fixture.lineage.node.base.id)
        );
        anyhow::ensure!(
            evidence.quote_hash == support::digest(support::TEXT)
                && evidence.start == 0
                && usize::try_from(evidence.end)? == support::TEXT.len()
        );
        anyhow::ensure!(
            !receipt.outbox_ids.is_empty(),
            "whole graph receipt omitted durable propagation"
        );
        anyhow::ensure!(fixture.publication.finalize().await? == receipt);
    }
    Ok(())
}

#[tokio::test]
async fn mutually_consistent_tampered_staging_cannot_invent_a_support_verdict() -> anyhow::Result<()>
{
    for backend in support::BACKENDS {
        let fixture = GraphFixture::new(support::storage(backend).await?, SPACE).await?;
        fixture.index_fixture_vectors().await?;
        let client = &fixture.publication.client;
        let manifests = SourcePublicationManifestRepository::new(client.clone());
        manifests
            .check_ready(
                &manifests
                    .requirements(SPACE, &fixture.publication.publication)
                    .await?,
            )
            .await?;
        let artifacts = WorkArtifactRepository::new(client.clone());
        let (candidate, bundle) = &fixture.publication.artifacts[1];
        for (id, is_bundle) in [(&candidate.base.id, false), (&bundle.base.id, true)] {
            let mut row = artifacts
                .get(SPACE, id)
                .await?
                .ok_or_else(|| anyhow::anyhow!("staged fixture disappeared"))?;
            let data = if is_bundle {
                &mut row.data["payload"]["data"]
            } else {
                &mut row.data
            };
            data["assertions"][0]["support_verification"] = Value::Null;
            data["assertions"][0]["predicate_text"] = json!("closed");
            artifacts.update(SPACE, id, row).await?;
        }
        anyhow::ensure!(
            fixture.publication.finalize().await.is_err(),
            "matching candidate+bundle bypassed independent semantic support"
        );
        no_graph(&fixture).await?;
    }
    Ok(())
}

fn reader() -> Principal {
    Principal {
        subject: "qualification-authorized-reader".into(),
        space_id: Some(SPACE.into()),
        scopes: vec![
            "read:doc:raw".into(),
            "read:entity".into(),
            "read:assertion".into(),
        ],
        clearances: vec![elephant_policy::Clearance {
            policy_id: "qualification-policy".into(),
            max_level: 7,
            labels: HashSet::from(["research".into()]),
            handling_allow: HashSet::from(["no-external-processing".into()]),
        }],
        ..Principal::default()
    }
}

async fn public_search(fixture: &GraphFixture, input: Value) -> anyhow::Result<Value> {
    let authz = Arc::new(AuthzService::new());
    let client = &fixture.publication.client;
    let app = elephant_api::routes::scoped_search::router(client.clone(), authz.clone())
        .layer(Extension(reader()));
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/v1/spaces/{SPACE}/search"))
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&input)?))?,
        )
        .await?;
    anyhow::ensure!(response.status() == StatusCode::OK);
    let body: Value = serde_json::from_slice(&to_bytes(response.into_body(), 262_144).await?)?;
    let mut dispatcher = ToolDispatcher::new(ToolContext::new(
        client.clone(),
        authz.to_abac_principal(&reader()),
    ));
    dispatcher.register(elephant_mcp::scoped_search::ScopedSearchHandler);
    anyhow::ensure!(
        support::mcp_success_data(dispatcher.execute("search_scoped", input).await?)? == body,
        "HTTP/MCP exact evidence differs"
    );
    Ok(body)
}

#[tokio::test]
async fn public_lexical_read_returns_exact_published_bytes_and_never_substitutes_pending_revision(
) -> anyhow::Result<()> {
    for backend in support::BACKENDS {
        let fixture = GraphFixture::new(support::storage(backend).await?, SPACE).await?;
        fixture.index_fixture_vectors().await?;
        fixture.publication.finalize().await?;
        let source_id = "qualification/oriole/opening";
        let request = json!({"query":"Oriole","limit":3,"use_vector":false,"source_revision_constraints":{"qualification/oriole/opening":"oriole-revision-1"}});
        let body = public_search(&fixture, request).await?;
        anyhow::ensure!(
            body["partial"] == false && body["coverage"][source_id]["state"] == "complete"
        );
        let results = body["results"]
            .as_array()
            .ok_or_else(|| anyhow::anyhow!("results missing"))?;
        anyhow::ensure!(!results.is_empty() && results.len() <= 3);
        for result in results {
            anyhow::ensure!(result.get("vector").is_none() && result.get("embedding").is_none());
            let evidence = result["evidence"]
                .as_array()
                .ok_or_else(|| anyhow::anyhow!("immutable evidence missing"))?;
            anyhow::ensure!(!evidence.is_empty());
            for reference in evidence {
                anyhow::ensure!(
                    reference["source_id"] == source_id
                        && reference["revision"] == "oriole-revision-1"
                );
                let start = usize::try_from(
                    reference["span"]["start"]
                        .as_u64()
                        .ok_or_else(|| anyhow::anyhow!("span start missing"))?,
                )?;
                let end = usize::try_from(
                    reference["span"]["end"]
                        .as_u64()
                        .ok_or_else(|| anyhow::anyhow!("span end missing"))?,
                )?;
                anyhow::ensure!(
                    start < end && support::TEXT.get(start..end) == reference["quote"].as_str(),
                    "public quote is not an exact immutable UTF-8 slice"
                );
            }
        }
        let mut next = fixture.publication.request.clone();
        next.revision_id = "oriole-revision-2".into();
        next.idempotency_key = "oriole-request-2".into();
        next.request_fingerprint = "oriole-exact-replacement-2".into();
        next.expected =
            SourcePrecondition::Generation(fixture.publication.accepted.source_generation.clone());
        let sources = SourcePublicationRepository::new(fixture.publication.client.clone());
        next.authorized = Some(
            support::authorized_source_snapshot(
                &sources,
                SPACE,
                &fixture.publication.accepted.source_id,
            )
            .await?,
        );
        sources.accept(next).await?;
        let pending = public_search(&fixture, json!({"query":"Oriole","limit":3,"use_vector":false,"source_revision_constraints":{"qualification/oriole/opening":"oriole-revision-2"}})).await?;
        anyhow::ensure!(pending["results"] == json!([]) && pending["partial"] == true);
        anyhow::ensure!(
            pending["coverage"][source_id]["desired_revision"] == "oriole-revision-2"
                && pending["coverage"][source_id]["published_revision"] == "oriole-revision-1"
        );
        anyhow::ensure!(pending["coverage"][source_id]["state"] != "complete");
    }
    Ok(())
}

#[tokio::test]
async fn public_claim_evidence_keeps_support_beyond_an_unrelated_quote_prefix() -> anyhow::Result<()>
{
    for backend in support::BACKENDS {
        let client = support::storage(backend).await?;
        elephant_storage::SpaceConfigRepository::new(client.clone())
            .create(graph_support::configuration(SPACE))
            .await?;
        let text = support::long_evidence_text();
        let supporting_start = text
            .find(support::TEXT)
            .ok_or_else(|| anyhow::anyhow!("fixed support clause absent"))?;
        anyhow::ensure!(supporting_start > 1024 && text.len() < 2048);
        let lineage = support::lineage_text_for_registered_space(
            &client,
            SPACE,
            elephant_types::records::base::SubjectsState::Complete,
            "qualification/oriole/long-evidence",
            "long-evidence-r1",
            &text,
        )
        .await?;
        let fixture = GraphFixture::from_lineage(client, SPACE, lineage).await?;
        fixture.index_fixture_vectors().await?;
        fixture.publication.finalize().await?;
        let body = public_search(&fixture, json!({
            "query":"opened", "limit":20, "use_vector":false,
            "source_revision_constraints":{"qualification/oriole/long-evidence":"long-evidence-r1"}
        })).await?;
        anyhow::ensure!(body["partial"] == false);
        let results = body["results"]
            .as_array()
            .ok_or_else(|| anyhow::anyhow!("public results absent"))?;
        let claim = results
            .iter()
            .find(|row| row["kind"] == "claim")
            .ok_or_else(|| anyhow::anyhow!("exact authorized published opening claim absent"))?;
        let claim_text = claim["text"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("claim text absent"))?;
        anyhow::ensure!(claim_text.contains("Élodie") && claim_text.contains("opened"));
        let evidence = claim["evidence"]
            .as_array()
            .ok_or_else(|| anyhow::anyhow!("claim evidence absent"))?;
        let mut supported = false;
        for reference in evidence {
            anyhow::ensure!(
                reference["source_id"] == "qualification/oriole/long-evidence"
                    && reference["revision"] == "long-evidence-r1"
            );
            let start = usize::try_from(
                reference["span"]["start"]
                    .as_u64()
                    .ok_or_else(|| anyhow::anyhow!("evidence start absent"))?,
            )?;
            let end = usize::try_from(
                reference["span"]["end"]
                    .as_u64()
                    .ok_or_else(|| anyhow::anyhow!("evidence end absent"))?,
            )?;
            let quote = reference["quote"]
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("evidence quote absent"))?;
            anyhow::ensure!(start < end && text.get(start..end) == Some(quote));
            supported |= quote.contains("Élodie opened the Oriole laboratory");
        }
        anyhow::ensure!(
            supported,
            "public claim only returned an unrelated prefix of its verified supporting span"
        );
    }
    Ok(())
}
