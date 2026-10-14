//! Actual public MCP search over independently frozen raw/rendered coordinates.
//! The provider supplies transport-spy vectors only; this is not relevance evidence.

use super::{index_fixture as fixture, spy};
use anyhow::Context;
use elephant_mcp::handlers::ToolDispatcher;
use elephant_mcp::scoped_search::{ScopedSearchHandler, SearchResponse};
use elephant_pipeline::provider_dispatch::with_dispatch_credentials;
use elephant_storage::{DocRevisionRepository, Repository, SpaceConfigRepository, StorageClient};
use elephant_types::records::doc_revision::RendererMetadata;
use elephant_types::RecordId;
use serde::Deserialize;
use serde_json::json;
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, sync::Arc};

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

fn hash(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

// Explicit mechanics inputs declared before publication. These vectors do
// not predict relevance: they select the second physical node for its literal
// coordinate oracle, through the real index and governed query adapter.
fn declared_vectors(
    oracle: &Oracle,
    dimensions: usize,
) -> anyhow::Result<BTreeMap<String, Vec<f32>>> {
    let mut other = vec![0.0; dimensions];
    other[1] = 1.0;
    let mut selected = vec![0.0; dimensions];
    selected[0] = 1.0;
    let mut vectors = BTreeMap::new();
    for case in &oracle.positive_cases {
        let mut cuts = vec![
            0,
            case.rendered_span.start,
            case.rendered_span.end,
            case.rendered_text.len(),
        ];
        cuts.sort_unstable();
        cuts.dedup();
        for pair in cuts.windows(2) {
            let text = case
                .rendered_text
                .get(pair[0]..pair[1])
                .context("invalid independent node span")?;
            if !text.trim().is_empty() {
                vectors.insert(text.to_owned(), other.clone());
            }
        }
    }
    let target = oracle
        .positive_cases
        .iter()
        .find(|case| case.id == "second_repeated_phrase_after_crlf_trim")
        .context("independent repeated occurrence case missing")?;
    vectors.insert(target.rendered_quote.clone(), selected.clone());
    vectors.insert("Helios".into(), selected);
    Ok(vectors)
}

async fn query(
    storage: &StorageClient,
    space: &str,
    case: &Case,
) -> anyhow::Result<serde_json::Value> {
    // Fixed lexical prompts select support; no model or result-derived query.
    let query = match case.id.as_str() {
        "unicode_after_lone_cr_and_crlf" => "café",
        "unicode_trailing_whitespace" => "Next fact",
        _ => "Helios",
    };
    let mut dispatcher = ToolDispatcher::new(fixture::reader(storage, space));
    dispatcher.register(ScopedSearchHandler);
    Ok(with_dispatch_credentials(
        Arc::new(spy::SpyCredentials),
        dispatcher.execute(
            "search_scoped",
            json!({
                "query":query, "limit":50,
                "ranking_mode": if case.id == "second_repeated_phrase_after_crlf_trim" { "semantic" } else { "lexical" },
                "source_revision_constraints":{(case.id.clone()):"original-r1"}
            }),
        ),
    )
    .await?)
}

#[tokio::test]
async fn actual_search_preserves_literal_raw_and_rendered_coordinates() -> anyhow::Result<()> {
    Box::pin(coordinate_contract()).await
}

#[allow(clippy::too_many_lines)]
async fn coordinate_contract() -> anyhow::Result<()> {
    anyhow::ensure!(
        hash(ORACLE.as_bytes()) == ORACLE_SHA,
        "independent fixture revision changed; review it before execution"
    );
    let oracle: Oracle = serde_json::from_str(ORACLE)?;
    anyhow::ensure!(oracle.positive_cases.len() == 7);
    for backend in spy::BACKENDS {
        let storage = Box::pin(spy::storage(backend)).await?;
        let configurations = SpaceConfigRepository::new(storage.clone());
        let endpoints = [spy::Spy::start().await?, spy::Spy::start().await?];
        for (index, space) in spy::SPACES.into_iter().enumerate() {
            Box::pin(spy::configure(
                &configurations,
                space,
                &endpoints[index].endpoint,
                8 + index * 4,
            ))
            .await?;
            endpoints[index].declared_vectors(declared_vectors(&oracle, 8 + index * 4)?)?;
            for case in &oracle.positive_cases {
                // Assertions use D's literal bytes and offsets. The product
                // mapper/renderer is never used to derive an expected answer.
                anyhow::ensure!(hash(case.accepted_text.as_bytes()) == case.accepted_utf8_sha256);
                anyhow::ensure!(hash(case.rendered_text.as_bytes()) == case.rendered_utf8_sha256);
                anyhow::ensure!(
                    case.accepted_text
                        .get(case.raw_span.start..case.raw_span.end)
                        == Some(case.raw_quote.as_str())
                );
                anyhow::ensure!(
                    case.rendered_text
                        .get(case.rendered_span.start..case.rendered_span.end)
                        == Some(case.rendered_quote.as_str())
                );
                let mut cuts: Vec<_> = [case.rendered_span.start, case.rendered_span.end]
                    .into_iter()
                    .filter(|point| *point > 0 && *point < case.rendered_text.len())
                    .collect();
                cuts.sort_unstable();
                cuts.dedup();
                with_dispatch_credentials(
                    Arc::new(spy::SpyCredentials),
                    Box::pin(fixture::publish_default_text(
                        &storage,
                        space,
                        &case.id,
                        "original-r1",
                        &case.accepted_text,
                        &case.rendered_text,
                        fixture::security(
                            if index == 0 {
                                "synthetic-household"
                            } else {
                                "synthetic-business"
                            },
                            if index == 0 { 3 } else { 7 },
                        ),
                        &cuts,
                    )),
                )
                .await?;
                let before = [endpoints[0].count()?, endpoints[1].count()?];
                let response = Box::pin(query(&storage, space, case)).await?;
                anyhow::ensure!(
                    response["ok"] == true,
                    "{backend:?}/{space}/{}: actual MCP search refused: {response}",
                    case.id
                );
                let response: SearchResponse = serde_json::from_value(response["data"].clone())?;
                let evidence: Vec<_> = response
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
                    evidence.len() == 1,
                    "{backend:?}/{space}/{}: original selected interval missing or duplicated",
                    case.id
                );
                let evidence = evidence[0];
                anyhow::ensure!(
                    evidence.revision == "original-r1"
                        && evidence.span.start as usize == case.raw_span.start
                        && evidence.span.end as usize == case.raw_span.end
                        && evidence.quote == case.raw_quote
                        && hash(evidence.quote.as_bytes()) == case.raw_quote_sha256,
                    "raw support changed meaning for {}",
                    case.id
                );
                anyhow::ensure!(
                    evidence.rendered_quote_sha256 == case.rendered_quote_sha256
                        && evidence.rendered_text_sha256 == case.rendered_utf8_sha256,
                    "rendered support was silently relabeled for {}",
                    case.id
                );
                let id: RecordId = evidence.internal_doc_revision_id.parse()?;
                let revisions = DocRevisionRepository::new(storage.clone());
                let revision = revisions
                    .get(space, &id)
                    .await?
                    .context("original immutable render missing")?;
                anyhow::ensure!(
                    revision.text == case.rendered_text
                        && revision.renderer == oracle.renderer
                        && revision.text_hash == case.rendered_utf8_sha256
                        && evidence.doc_id == revision.doc.id().to_string()
                        && evidence.doc_id.len() == 40
                        && evidence.doc_id.parse::<RecordId>()?.table()
                            == elephant_types::TableName::Doc,
                    "public evidence bound another render identity"
                );
                with_dispatch_credentials(
                    Arc::new(spy::SpyCredentials),
                    Box::pin(super::render_coordinates_http::verify_original_http_case(
                        &storage,
                        space,
                        &case.id,
                        revision.doc.id(),
                        &id,
                    )),
                )
                .await?;
                let mut expected_queries = before;
                if case.id == "second_repeated_phrase_after_crlf_trim" {
                    expected_queries[index] += 2; // One actual MCP and one actual HTTP query.
                }
                anyhow::ensure!(
                    [endpoints[0].count()?, endpoints[1].count()?] == expected_queries,
                    "coordinate query dispatch count differs from the declared transport recipe"
                );

                // A self-consistent replacement cannot borrow the completed
                // original render proof. No canonical receipt is synthesized.
                let mut tampered = revision;
                tampered.text.push_str("Unaccepted replacement.");
                tampered.text_hash = hash(tampered.text.as_bytes());
                revisions.update(space, &id, tampered).await?;
                let refused = Box::pin(query(&storage, space, case)).await?;
                // The same MCP caller/input/binding reuses its earlier vector,
                // while current source reads must still reject the tampered
                // render. The cache retains neither result nor read authority.
                // The original MCP and HTTP cold queries above still require
                // exactly one physical dispatch each; this recheck requires zero.
                anyhow::ensure!(
                    [endpoints[0].count()?, endpoints[1].count()?] == expected_queries,
                    "tamper recheck unexpectedly redispatched the original caller query"
                );
                if refused["ok"] == true {
                    let refused: SearchResponse = serde_json::from_value(refused["data"].clone())?;
                    anyhow::ensure!(
                        refused.results.iter().all(|result| result
                            .evidence
                            .iter()
                            .all(|evidence| evidence.source_id != case.id)),
                        "tampered rendered bytes borrowed original completion"
                    );
                }
            }
        }
    }
    Ok(())
}
