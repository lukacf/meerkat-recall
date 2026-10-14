//! Mounted HTTP search and immutable GET over S's same actual publication.
//! Expected coordinates come from D's original literal bytes, never the mapper.

use anyhow::Context;
use axum::{
    body::{to_bytes, Body},
    http::{Request, StatusCode},
    Extension,
};
use elephant_api::{
    routes::{
        docs::{doc_routes, DocsState},
        scoped_search,
    },
    AuthzService, Principal,
};
use elephant_mcp::scoped_search::SearchResponse;
use elephant_policy::Clearance;
use elephant_storage::StorageClient;
use elephant_types::records::{doc_revision::RendererMetadata, DocRevision};
use elephant_types::RecordId;
use serde::Deserialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{collections::HashSet, sync::Arc};
use tower::ServiceExt;

const ORACLE: &str = include_str!("../recall_preview/render-coordinate-fixtures.json");
const ORACLE_SHA: &str = "54138704a332d6e275dc434ad0d6456f49107a811c08ea9abbafdbd68171dc3b";

#[derive(Deserialize)]
struct Oracle {
    renderer: RendererMetadata,
    positive_cases: Vec<Case>,
}
#[derive(Deserialize)]
struct Span {
    start: usize,
    end: usize,
}
#[derive(Deserialize)]
struct Case {
    id: String,
    accepted_text: String,
    accepted_utf8_sha256: String,
    rendered_text: String,
    rendered_utf8_sha256: String,
    raw_span: Span,
    raw_quote: String,
    raw_quote_sha256: String,
    rendered_span: Span,
    rendered_quote: String,
    rendered_quote_sha256: String,
}

fn hash(text: &str) -> String {
    format!("{:x}", Sha256::digest(text.as_bytes()))
}

fn reader(space: &str) -> Principal {
    Principal {
        subject: "D-coordinate-http-reader".into(),
        space_id: Some(space.into()),
        scopes: vec![
            "read:doc".into(),
            "read:doc:raw".into(),
            "read:entity".into(),
            "read:assertion".into(),
        ],
        // The original provider fixture creates its SpaceConfig under default/0.
        // Source/Doc evidence remains classified under the two explicit policies.
        // All clearances are fixed before reading any returned record.
        clearances: [
            ("default", 0),
            ("synthetic-household", 3),
            ("synthetic-business", 7),
        ]
        .into_iter()
        .map(|(policy, level)| Clearance {
            policy_id: policy.into(),
            max_level: level,
            labels: HashSet::new(),
            handling_allow: HashSet::new(),
        })
        .collect(),
        ..Principal::default()
    }
}

async fn exchange(
    storage: &StorageClient,
    principal: Principal,
    method: &str,
    path: &str,
    body: Option<Value>,
    limit: usize,
) -> anyhow::Result<(StatusCode, Value)> {
    let authz = Arc::new(AuthzService::new());
    let app = scoped_search::router(storage.clone(), authz.clone())
        .merge(doc_routes().with_state(Arc::new(DocsState::new(storage, authz))))
        .layer(Extension(principal));
    let body = body.map_or_else(|| Ok(Vec::new()), |value| serde_json::to_vec(&value))?;
    let response = app
        .oneshot(
            Request::builder()
                .method(method)
                .uri(path)
                .header("content-type", "application/json")
                .body(Body::from(body))?,
        )
        .await?;
    let status = response.status();
    let raw = to_bytes(response.into_body(), limit).await?;
    Ok((status, serde_json::from_slice(&raw)?))
}

/// Called by the actual MCP coordinate fixture before its tamper negative.
/// No separate publication, model request, direct record substitution or bypass.
pub async fn verify_original_http_case(
    storage: &StorageClient,
    space: &str,
    case_id: &str,
    doc_id: &RecordId,
    revision_id: &RecordId,
) -> anyhow::Result<()> {
    anyhow::ensure!(hash(ORACLE) == ORACLE_SHA, "D literal source bytes changed");
    let oracle: Oracle = serde_json::from_str(ORACLE)?;
    let case = oracle
        .positive_cases
        .iter()
        .find(|case| case.id == case_id)
        .context("unreviewed coordinate case")?;
    anyhow::ensure!(hash(&case.accepted_text) == case.accepted_utf8_sha256);
    anyhow::ensure!(hash(&case.raw_quote) == case.raw_quote_sha256);
    anyhow::ensure!(
        case.accepted_text
            .get(case.raw_span.start..case.raw_span.end)
            == Some(case.raw_quote.as_str())
    );
    let query = match case.id.as_str() {
        "unicode_after_lone_cr_and_crlf" => "café",
        "unicode_trailing_whitespace" => "Next fact",
        _ => "Helios",
    };
    // Lexical search retains the first matching chunk per document. Only the
    // repeated-phrase case uses the shared fixture's actual indexed semantic
    // selection, with one governed query dispatch and fixed transport vectors.
    let ranking_mode = if case.id == "second_repeated_phrase_after_crlf_trim" {
        "semantic"
    } else {
        "lexical"
    };
    let request = json!({
        "query": query, "limit": 50, "ranking_mode": ranking_mode,
        "source_revision_constraints": {case.id.clone(): "original-r1"}
    });
    let path = format!("/v1/spaces/{space}/search");
    // Keep the original failure as a mounted authorization control: classified
    // document clearance alone does not authorize the selected configuration.
    let mut no_config_clearance = reader(space);
    no_config_clearance
        .clearances
        .retain(|clearance| clearance.policy_id != "default");
    let (status, body) = exchange(
        storage,
        no_config_clearance,
        "POST",
        &path,
        Some(request.clone()),
        262_144,
    )
    .await?;
    anyhow::ensure!(
        status == StatusCode::FORBIDDEN && body["error_code"] == "forbidden_space",
        "HTTP configuration policy was not enforced before evidence search"
    );
    let (status, body) = exchange(
        storage,
        reader(space),
        "POST",
        &path,
        Some(request.clone()),
        262_144,
    )
    .await?;
    anyhow::ensure!(
        status == StatusCode::OK,
        "mounted HTTP search refused {}: {status} {body}",
        case.id
    );
    let response: SearchResponse = serde_json::from_value(body)?;
    let support: Vec<_> = response
        .results
        .iter()
        .flat_map(|result| &result.evidence)
        .filter(|evidence| {
            evidence.source_id == case.id
                && evidence.rendered_span.start as usize == case.rendered_span.start
                && evidence.rendered_span.end as usize == case.rendered_span.end
        })
        .collect();
    anyhow::ensure!(
        support.len() == 1,
        "HTTP lost or duplicated original support"
    );
    let support = support[0];
    anyhow::ensure!(
        support.revision == "original-r1"
            && support.doc_id == doc_id.to_string()
            && support.internal_doc_revision_id == revision_id.to_string()
            && support.span.start as usize == case.raw_span.start
            && support.span.end as usize == case.raw_span.end
            && support.quote == case.raw_quote
            && support.rendered_quote_sha256 == case.rendered_quote_sha256
            && support.rendered_text_sha256 == case.rendered_utf8_sha256,
        "HTTP citation coordinates or original revision changed"
    );
    let revision_path = format!(
        "/v1/docs/{}/revisions/{}",
        support.doc_id, support.internal_doc_revision_id
    );
    let (status, body) = exchange(
        storage,
        reader(space),
        "GET",
        &revision_path,
        None,
        12_065_536,
    )
    .await?;
    anyhow::ensure!(
        status == StatusCode::OK,
        "original full revision refused: {body}"
    );
    let revision: DocRevision = serde_json::from_value(body)?;
    anyhow::ensure!(
        revision.base.id == *revision_id
            && revision.doc.id() == doc_id
            && revision.base.space_id == space
            && revision.text == case.rendered_text
            && revision.renderer == oracle.renderer
            && hash(&revision.text) == support.rendered_text_sha256
            && revision.text_hash == support.rendered_text_sha256,
        "full immutable HTTP DTO did not bind the actual cited render"
    );
    let rendered_support = revision
        .text
        .get(support.rendered_span.start as usize..support.rendered_span.end as usize)
        .context("invalid immutable rendered UTF-8 boundary")?;
    anyhow::ensure!(
        rendered_support == case.rendered_quote
            && hash(rendered_support) == support.rendered_quote_sha256
    );
    let mut no_raw = reader(space);
    no_raw.scopes = vec!["read:doc".into()];
    let (status, body) = exchange(storage, no_raw, "GET", &revision_path, None, 262_144).await?;
    anyhow::ensure!(status == StatusCode::FORBIDDEN && body["error_code"] == "forbidden_space");
    let mut foreign = reader(space);
    foreign.space_id = Some("not-the-selected-coordinate-space".into());
    let (status, body) = exchange(storage, foreign, "POST", &path, Some(request), 262_144).await?;
    anyhow::ensure!(status == StatusCode::FORBIDDEN && body["error_code"] == "forbidden_space");
    Ok(())
}
