//! Actual scoped engine and physically indexed negative controls.
//! Controlled vectors establish transport/ranking behavior only. Never relevance.
use super::{index_fixture as fixture, spy};
use anyhow::Context;
use elephant_mcp::handlers::ToolDispatcher;
use elephant_mcp::scoped_search::{ScopedSearchHandler, SearchKind, SearchResponse};
use elephant_pipeline::provider_dispatch::with_dispatch_credentials;
use elephant_storage::SpaceConfigRepository;
use elephant_types::provider_binding::ModelOperation;
use serde::Deserialize;
use serde_json::json;
use sha2::{Digest, Sha256};
use spy::{BACKENDS, SPACES};
use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

async fn query(
    client: &elephant_storage::StorageClient,
    space: &str,
    value: serde_json::Value,
) -> anyhow::Result<SearchResponse> {
    let mut dispatcher = ToolDispatcher::new(fixture::reader(client, space));
    dispatcher.register(ScopedSearchHandler);
    let response = with_dispatch_credentials(
        Arc::new(spy::SpyCredentials),
        dispatcher.execute("search_scoped", value),
    )
    .await?;
    anyhow::ensure!(response["ok"] == true, "scoped handler failed: {response}");
    Ok(serde_json::from_value(response["data"].clone())?)
}

fn direction(x: f32, y: f32) -> Vec<f32> {
    let mut vector = vec![0.0; 8];
    vector[0] = x;
    vector[1] = y;
    vector
}

#[tokio::test]
async fn actual_index_semantic_mode_preserves_matched_node_and_global_similarity(
) -> anyhow::Result<()> {
    for backend in BACKENDS {
        let client = spy::storage(backend).await?;
        let config = SpaceConfigRepository::new(client.clone());
        let endpoint = spy::Spy::start().await?;
        spy::configure(&config, SPACES[0], &endpoint.endpoint, 8).await?;
        let original = "red lever location";
        let matched = "Shut the outdoor supply with the valve in the utility cupboard.\n";
        let lexical = "A fictional red lever location appears in this story.";
        let other = "A second physically indexed source in another policy partition.";
        let stale = "The obsolete high-similarity source must not be returned.";
        let current = "This is the current source revision with a lower declared similarity.";
        let hidden = "This classified source must not appear in results or coverage.";
        endpoint.declared_vectors(BTreeMap::from([
            (original.into(), direction(1.0, 0.0)),
            (matched.into(), direction(1.0, 0.0)),
            (lexical.into(), direction(0.0, 1.0)),
            (other.into(), direction(0.8, 0.6)),
            (stale.into(), direction(1.0, 0.0)),
            (current.into(), direction(0.6, 0.8)),
            (hidden.into(), direction(1.0, 0.0)),
        ]))?;
        let text = format!("{matched}{lexical}");
        fixture::publish(
            &client,
            SPACES[0],
            "matched",
            "match-r1",
            &text,
            fixture::security("index-a", 0),
            &[matched.len()],
        )
        .await?;
        fixture::publish(
            &client,
            SPACES[0],
            "other-partition",
            "other-r1",
            other,
            fixture::security("index-b", 0),
            &[],
        )
        .await?;
        fixture::publish(
            &client,
            SPACES[0],
            "changed",
            "old",
            stale,
            fixture::security("default", 0),
            &[],
        )
        .await?;
        fixture::publish(
            &client,
            SPACES[0],
            "changed",
            "new",
            current,
            fixture::security("default", 0),
            &[],
        )
        .await?;
        fixture::publish(
            &client,
            SPACES[0],
            "hidden",
            "hidden-r1",
            hidden,
            fixture::security("index-hidden", 9),
            &[],
        )
        .await?;
        let before = endpoint.count()?;
        let response = query(
            &client,
            SPACES[0],
            json!({"query":original,"ranking_mode":"semantic","limit":3}),
        )
        .await?;
        anyhow::ensure!(endpoint.count()? == before + 1);
        anyhow::ensure!(response.results.len() == 3 && !response.partial);
        let expected = [
            ("matched", "match-r1", matched),
            ("other-partition", "other-r1", other),
            ("changed", "new", current),
        ];
        for (result, (source, revision, quote)) in response.results.iter().zip(expected) {
            anyhow::ensure!(matches!(result.kind, SearchKind::Chunk));
            anyhow::ensure!(result.text == quote && result.evidence.len() == 1);
            let evidence = &result.evidence[0];
            anyhow::ensure!(evidence.source_id == source && evidence.revision == revision);
            anyhow::ensure!(
                evidence.span.start == 0 && usize::try_from(evidence.span.end)? == quote.len()
            );
        }
        anyhow::ensure!(
            response.results[0].score > response.results[1].score
                && response.results[1].score > response.results[2].score,
            "partition-local rank replaced global vector similarity"
        );
        anyhow::ensure!(!response.coverage.contains_key("hidden"));
        // The lexically matching sibling node must not replace the vector hit.
        anyhow::ensure!(response.results[0].text != lexical);
        // Hybrid must assign global document ranks after current evidence
        // eligibility: the old high-scoring revision and sibling chunk cannot
        // add votes, and the second policy cannot restart the rank at zero.
        let hybrid = query(
            &client,
            SPACES[0],
            json!({"query":original,"ranking_mode":"hybrid","limit":3}),
        )
        .await?;
        for (result, (source, score)) in hybrid.results.iter().zip([
            ("matched", 2.0),
            ("other-partition", 1.0 / 2.0),
            ("changed", 1.0 / 3.0),
        ]) {
            anyhow::ensure!(result.evidence[0].source_id == source);
            anyhow::ensure!(
                (result.score - score).abs() < 1e-12,
                "hybrid counted a stale/sibling/partition vote: {result:?}"
            );
        }
        anyhow::ensure!(hybrid.results.len() == 3 && !hybrid.coverage.contains_key("hidden"));
        anyhow::ensure!(
            hybrid.results[0].text == matched,
            "hybrid substituted the lexical sibling for its actual semantic support"
        );
        spy::edit(&config, SPACES[0], |processing| {
            processing
                .bindings
                .get_mut(&ModelOperation::QueryEmbedding)
                .context("missing query binding")?
                .revoked = true;
            Ok(())
        })
        .await?;
        let before = endpoint.count()?;
        for mode in ["semantic", "hybrid"] {
            anyhow::ensure!(query(
                &client,
                SPACES[0],
                json!({"query":original,"ranking_mode":mode})
            )
            .await
            .is_err());
            anyhow::ensure!(endpoint.count()? == before);
        }
        query(
            &client,
            SPACES[0],
            json!({"query":original,"ranking_mode":"lexical"}),
        )
        .await?;
        query(
            &client,
            SPACES[0],
            json!({"query":original,"use_vector":false}),
        )
        .await?;
        anyhow::ensure!(
            endpoint.count()? == before,
            "lexical mode resolved a model after revocation"
        );
        anyhow::ensure!(query(
            &client,
            SPACES[0],
            json!({"query":original,"ranking_mode":"semantic","use_vector":false})
        )
        .await
        .is_err());
    }
    Ok(())
}

/// Equal immutable revision names do not identify the same acceptance attempt.
/// Publish through real render/embed/finalize, then accept B and a fresh A while
/// keeping the original A index readable. No head/receipt is manufactured.
#[tokio::test]
async fn pending_aba_acceptance_keeps_old_index_evidence_with_partial_coverage(
) -> anyhow::Result<()> {
    use elephant_storage::source_publication::{AcceptSourceRevision, SourcePublicationRepository};
    use elephant_types::publication::{OperationState, SourcePrecondition};
    use elephant_types::records::SourceType;
    for backend in BACKENDS {
        let client = spy::storage(backend).await?;
        let config = SpaceConfigRepository::new(client.clone());
        let endpoint = spy::Spy::start().await?;
        spy::configure(&config, SPACES[0], &endpoint.endpoint, 8).await?;
        let text = "The red lever is inside the utility cupboard.";
        let original_query = "red lever";
        endpoint.declared_vectors(BTreeMap::from([
            (text.into(), direction(1.0, 0.0)),
            (original_query.into(), direction(1.0, 0.0)),
        ]))?;
        fixture::publish(
            &client,
            SPACES[0],
            "aba",
            "revision-a",
            text,
            fixture::security("default", 0),
            &[],
        )
        .await?;
        let repository = SourcePublicationRepository::new(client.clone());
        let a_head = repository
            .external_head(SPACES[0], SourceType::Manual, "aba")
            .await?
            .context("published A head missing")?;
        let a = repository
            .revision(SPACES[0], &a_head.source_id, "revision-a")
            .await?
            .context("immutable A absent")?;
        let mut b_source = a.source.clone();
        let mut template = b_source
            .manual_doc_template()?
            .context("A template absent")?;
        template.text = "A later proposal would move the lever to the hallway.".into();
        template.doc_id =
            elephant_types::RecordId::new_v4(elephant_types::TableName::Doc).to_string();
        template.rendition_key = "aba/revision-b".into();
        b_source.hash = fixture::digest(&template.text);
        b_source.set_manual_doc_template(&template)?;
        let b = repository
            .accept(AcceptSourceRevision {
                source: b_source,
                revision_id: "revision-b".into(),
                expected: SourcePrecondition::Generation(a_head.generation.clone()),
                idempotency_key: "semantic-aba/b".into(),
                request_fingerprint: fixture::digest(&template.text),
                reactivate: false,
                recreate_after_erasure: false,
                authorized: repository
                    .authorization_snapshot(SPACES[0], &a_head.source_id)
                    .await?,
                authorized_document: None,
            })
            .await?;
        let fresh_a = repository
            .accept(AcceptSourceRevision {
                source: a.source,
                revision_id: "revision-a".into(),
                expected: SourcePrecondition::Generation(b.source_generation.clone()),
                idempotency_key: "semantic-aba/fresh-a".into(),
                request_fingerprint: fixture::digest(text),
                reactivate: false,
                recreate_after_erasure: false,
                authorized: repository
                    .authorization_snapshot(SPACES[0], &a_head.source_id)
                    .await?,
                authorized_document: None,
            })
            .await?;
        anyhow::ensure!(
            fresh_a.operation_id != a_head.operation_id
                && fresh_a.source_generation != a_head.generation
        );
        anyhow::ensure!(fresh_a.state == OperationState::Accepted);
        for mode in ["lexical", "semantic"] {
            for constrained in [false, true] {
                let mut request = json!({"query":original_query,"ranking_mode":mode,"limit":3});
                if constrained {
                    request["source_revision_constraints"] = json!({"aba":"revision-a"});
                }
                let response = query(&client, SPACES[0], request).await?;
                anyhow::ensure!(response.partial,
                    "{backend:?}/{mode}/constrained={constrained}: current A operation is pending: {response:?}");
                let coverage = response.coverage.get("aba").context("A coverage absent")?;
                anyhow::ensure!(
                    coverage.state == "pending"
                        && coverage.published_revision.as_deref() == Some("revision-a")
                );
                anyhow::ensure!(
                    response
                        .results
                        .iter()
                        .any(|result| result
                            .evidence
                            .iter()
                            .any(|evidence| evidence.source_id == "aba"
                                && evidence.revision == "revision-a"
                                && evidence.quote == text)),
                    "pending A acceptance hid the previous published A evidence"
                );
            }
        }
        let operation = repository
            .operation(SPACES[0], &fresh_a.operation_id)
            .await?
            .context("fresh operation missing")?;
        anyhow::ensure!(
            operation.state == OperationState::Accepted && operation.completed_at.is_none(),
            "search fabricated acceptance completion"
        );
    }
    Ok(())
}

#[tokio::test]
async fn semantic_window_reaches_requested_source_after_401_ineligible_nodes() -> anyhow::Result<()>
{
    for backend in BACKENDS {
        let client = spy::storage(backend).await?;
        let config = SpaceConfigRepository::new(client.clone());
        let endpoint = spy::Spy::start().await?;
        spy::configure(&config, SPACES[0], &endpoint.endpoint, 8).await?;
        let original = "location of the orchard isolation valve";
        let target = "The orchard isolation valve is beside the eastern pump.";
        let mut vectors = BTreeMap::from([
            (original.into(), direction(1.0, 0.0)),
            (target.into(), direction(0.6, 0.8)),
        ]);
        let mut text = String::new();
        let mut cuts = Vec::new();
        for ordinal in 0..401 {
            let line = format!("Unrequested storage-window distractor {ordinal:03}.\n");
            vectors.insert(line.clone(), direction(1.0, 0.0));
            text.push_str(&line);
            if ordinal != 400 {
                cuts.push(text.len());
            }
        }
        endpoint.declared_vectors(vectors)?;
        fixture::publish(
            &client,
            SPACES[0],
            "window-target",
            "target-r1",
            target,
            fixture::security("default", 0),
            &[],
        )
        .await?;
        fixture::publish(
            &client,
            SPACES[0],
            "window-unrequested",
            "unrequested-r1",
            &text,
            fixture::security("default", 0),
            &cuts,
        )
        .await?;
        // Both publications and all 402 chunks use the same allowed partition.
        // The requested lower-scoring target lies beyond the old raw 400 limit.
        let before = endpoint.count()?;
        let response = query(
            &client,
            SPACES[0],
            json!({
                "query":original,"ranking_mode":"semantic","limit":1,
                "source_revision_constraints":{"window-target":"target-r1"}
            }),
        )
        .await?;
        let hybrid = query(
            &client,
            SPACES[0],
            json!({
                "query":original,"ranking_mode":"hybrid","limit":1,
                "source_revision_constraints":{"window-target":"target-r1"}
            }),
        )
        .await?;
        anyhow::ensure!(hybrid.results.len() == 1);
        anyhow::ensure!(hybrid.results[0].evidence[0].source_id == "window-target");
        anyhow::ensure!(
            (hybrid.results[0].score - 2.0).abs() < 1e-12,
            "ineligible raw window hits must not depress the eligible document rank"
        );
        anyhow::ensure!(
            endpoint.count()? == before + 1,
            "window widening must reuse the one governed query vector"
        );
        anyhow::ensure!(!response.partial && response.results.len() == 1);
        let evidence = response.results[0]
            .evidence
            .first()
            .context("target evidence missing")?;
        anyhow::ensure!(
            evidence.source_id == "window-target"
                && evidence.revision == "target-r1"
                && evidence.quote == target
        );
        anyhow::ensure!(response.coverage["window-target"].state == "complete");
    }
    Ok(())
}

/// One long, lower-similarity document must not win by having more chunks.
/// These controlled vectors exercise fusion only, not model relevance.
#[tokio::test]
async fn hybrid_document_votes_ignore_chunk_multiplicity_and_unrelated_corpus() -> anyhow::Result<()>
{
    for backend in BACKENDS {
        let client = spy::storage(backend).await?;
        let config = SpaceConfigRepository::new(client.clone());
        let endpoint = spy::Spy::start().await?;
        spy::configure(&config, SPACES[0], &endpoint.endpoint, 8).await?;
        let original = "rrfvotingprobe";
        let first = "The violet instrument is stored in the laboratory.";
        let line = "A chapter about unrelated musical notation.\n";
        let later = "A separate lower-scoring landscape description.";
        let mut vectors = BTreeMap::from([
            (original.into(), direction(1.0, 0.0)),
            (first.into(), direction(1.0, 0.0)),
            (line.into(), direction(0.8, 0.6)),
            (later.into(), direction(0.6, 0.8)),
        ]);
        let text = line.repeat(13);
        vectors.insert(text.clone(), direction(0.8, 0.6));
        endpoint.declared_vectors(vectors)?;
        // Reverse score order at ingestion; policy discovery cannot decide rank.
        fixture::publish(
            &client,
            SPACES[0],
            "many-chunks",
            "r1",
            &text,
            fixture::security("index-a", 0),
            &(1..13).map(|index| index * line.len()).collect::<Vec<_>>(),
        )
        .await?;
        fixture::publish(
            &client,
            SPACES[0],
            "single-chunk",
            "r1",
            first,
            fixture::security("index-b", 0),
            &[],
        )
        .await?;
        for include_unrelated in [false, true] {
            if include_unrelated {
                fixture::publish(
                    &client,
                    SPACES[0],
                    "unrelated",
                    "r1",
                    later,
                    fixture::security("default", 0),
                    &[],
                )
                .await?;
            }
            let response = query(
                &client,
                SPACES[0],
                json!({"query":original,"ranking_mode":"hybrid","limit":3}),
            )
            .await?;
            anyhow::ensure!(response.results.len() == if include_unrelated { 3 } else { 2 });
            for (result, (source, score)) in response.results.iter().zip([
                ("single-chunk", 1.0),
                ("many-chunks", 1.0 / 2.0),
                ("unrelated", 1.0 / 3.0),
            ]) {
                anyhow::ensure!(result.evidence[0].source_id == source);
                anyhow::ensure!(
                    (result.score - score).abs() < 1e-12,
                    "chunk count or corpus order multiplied the document vote: {result:?}"
                );
            }
        }
    }
    Ok(())
}

/// Deliberately supplied vectors measure rank fusion, never provider relevance.
/// A query with no literal match in its semantic leader must survive agreement
/// among lower semantic ranks. This uses the actual publication/index/search
/// path in both backends and both independently approved named spaces.
#[tokio::test]
async fn hybrid_keeps_semantic_and_lexical_leaders_amid_weak_agreement() -> anyhow::Result<()> {
    for backend in BACKENDS {
        let client = spy::storage(backend).await?;
        let config = SpaceConfigRepository::new(client.clone());
        let endpoint = spy::Spy::start().await?;
        for space in SPACES {
            spy::configure(&config, space, &endpoint.endpoint, 8).await?;
            let original = "quartz beacon";
            let semantic = "The violet instrument belongs inside the laboratory cupboard.";
            let lexical = [
                "A quartz beacon appears in this fictional astronomy story.",
                "The quartz beacon is discussed in an unrelated music exercise.",
                "A historical quartz beacon was drawn on a landscape map.",
                "The quartz beacon occurs in a separate railway timetable.",
            ];
            let mut vectors = BTreeMap::from([
                (original.into(), direction(1.0, 0.0)),
                (semantic.into(), direction(1.0, 0.0)),
            ]);
            for (index, text) in lexical.iter().enumerate() {
                let x = [0.8, 0.6, 0.4, 0.2][index];
                vectors.insert((*text).into(), direction(x, 1.0));
            }
            endpoint.declared_vectors(vectors)?;
            fixture::publish(
                &client,
                space,
                "paraphrase-leader",
                "r1",
                semantic,
                fixture::security("index-a", 0),
                &[],
            )
            .await?;
            for (index, text) in lexical.iter().enumerate() {
                fixture::publish(
                    &client,
                    space,
                    &format!("literal-{index}"),
                    "r1",
                    text,
                    fixture::security("index-b", 0),
                    &[],
                )
                .await?;
            }
            let literal = query(
                &client,
                space,
                json!({"query":original,"ranking_mode":"lexical","limit":1}),
            )
            .await?;
            let literal_leader = &literal.results[0].evidence[0].source_id;
            let before = endpoint.count()?;
            let response = query(
                &client,
                space,
                json!({"query":original,"ranking_mode":"hybrid","limit":3}),
            )
            .await?;
            anyhow::ensure!(endpoint.count()? == before + 1);
            anyhow::ensure!(response.results.len() == 3 && !response.partial);
            let sources = response
                .results
                .iter()
                .map(|row| row.evidence[0].source_id.as_str())
                .collect::<HashSet<_>>();
            anyhow::ensure!(
                sources.contains("paraphrase-leader"),
                "lower-ranked agreement buried the semantic leader: {response:?}"
            );
            anyhow::ensure!(
                sources.contains(literal_leader.as_str()),
                "fusion discarded the literal leader: {response:?}"
            );
            let selected = response
                .results
                .iter()
                .find(|row| row.evidence[0].source_id == "paraphrase-leader")
                .context("semantic leader absent")?;
            anyhow::ensure!(selected.text == semantic && selected.evidence[0].quote == semantic);
            anyhow::ensure!(
                (selected.score - 1.0).abs() < 1e-12,
                "semantic-only document gained a lexical vote"
            );
        }
    }
    Ok(())
}

#[derive(Deserialize)]
struct Corpus {
    spaces: Vec<Space>,
    sources: Vec<Source>,
    queries: Vec<Query>,
    evidence: Vec<Passage>,
    metric: Metric,
}
#[derive(Deserialize)]
struct Space {
    id: String,
    policy: String,
    level: i32,
    guidance: String,
}
#[derive(Deserialize)]
struct Source {
    id: String,
    space: String,
    revision: String,
    text: String,
    state: Option<String>,
}
#[derive(Deserialize)]
struct Query {
    id: String,
    space: String,
    query: String,
    required: Vec<String>,
}
#[derive(Deserialize)]
struct Passage {
    id: String,
    space: String,
    source_id: String,
    revision: String,
    start: u32,
    end: u32,
}
#[derive(Deserialize)]
struct Metric {
    cutoff: usize,
    min_macro_reciprocal_rank: f64,
}

/// Counter-based seeded pseudo-random vectors. SHA-256 inputs are only seed,
/// stream ordinal and coordinate; source/query content never influences them.
/// This source-level index control is distinct from Python's frozen MT control.
fn random_vector(seed: u64, stream: u64, dimensions: usize) -> anyhow::Result<Vec<f32>> {
    let mut vector = Vec::with_capacity(dimensions);
    for coordinate in 0..dimensions {
        let mut hash = Sha256::new();
        hash.update(b"semantic-index-negative-v1");
        hash.update(seed.to_le_bytes());
        hash.update(stream.to_le_bytes());
        hash.update(u64::try_from(coordinate)?.to_le_bytes());
        let bytes = hash.finalize();
        let value = u64::from_le_bytes(bytes[..8].try_into()?);
        vector.push(((value >> 11) as f64 / ((1_u64 << 53) as f64) * 2.0 - 1.0) as f32);
    }
    let norm = vector
        .iter()
        .map(|value| f64::from(*value).powi(2))
        .sum::<f64>()
        .sqrt();
    anyhow::ensure!(norm.is_finite() && norm > 0.0);
    for value in &mut vector {
        *value = (f64::from(*value) / norm) as f32;
    }
    anyhow::ensure!(vector.iter().all(|x| x.is_finite()));
    Ok(vector)
}

#[tokio::test]
async fn indexed_random_vectors_fail_frozen_semantic_expectations_on_both_backends(
) -> anyhow::Result<()> {
    let corpus: Corpus = serde_json::from_str(include_str!("corpus.json"))?;
    for backend in BACKENDS {
        for seed in [17_u64, 219, 20_261_006] {
            let client = spy::storage(backend).await?;
            let configs = SpaceConfigRepository::new(client.clone());
            let mut endpoints = Vec::new();
            for (index, space) in corpus.spaces.iter().enumerate() {
                let dimensions = if index == 0 { 384 } else { 1536 };
                let endpoint = spy::Spy::start().await?;
                let observed =
                    spy::configure(&configs, &space.id, &endpoint.endpoint, dimensions).await?;
                let mut configured = observed.clone();
                configured.purpose = Some(space.guidance.clone());
                configured
                    .processing
                    .as_mut()
                    .context("processing config absent")?
                    .generation += 1;
                configs.replace_authorized(&observed, configured).await?;
                let mut vectors = BTreeMap::new();
                for (ordinal, source) in corpus
                    .sources
                    .iter()
                    .enumerate()
                    .filter(|(_, source)| source.space == space.id)
                {
                    vectors.insert(
                        source.text.clone(),
                        random_vector(seed, u64::try_from(ordinal)?, dimensions)?,
                    );
                }
                for (ordinal, query) in corpus
                    .queries
                    .iter()
                    .enumerate()
                    .filter(|(_, query)| query.space == space.id)
                {
                    vectors.insert(
                        query.query.clone(),
                        random_vector(seed, 10_000 + u64::try_from(ordinal)?, dimensions)?,
                    );
                }
                endpoint.declared_vectors(vectors)?;
                endpoints.push(endpoint);
            }
            // Includes original old revision then replacement; neither published
            // heads nor finalizer receipts are fabricated by the fixture.
            for source in &corpus.sources {
                let space = corpus
                    .spaces
                    .iter()
                    .find(|space| space.id == source.space)
                    .context("source space absent")?;
                fixture::publish(
                    &client,
                    &source.space,
                    &source.id,
                    &source.revision,
                    &source.text,
                    fixture::security(&space.policy, space.level),
                    &[],
                )
                .await?;
            }
            let mut all_required = true;
            let mut reciprocal_sum = 0.0;
            let mut positive_queries = 0;
            let mut cases = Vec::new();
            for original in &corpus.queries {
                let constraints: BTreeMap<_, _> = corpus
                    .sources
                    .iter()
                    .filter(|source| {
                        source.space == original.space
                            && source.state.as_deref() != Some("superseded")
                    })
                    .map(|source| (source.id.clone(), source.revision.clone()))
                    .collect();
                let before = endpoints
                    .iter()
                    .map(spy::Spy::count)
                    .collect::<anyhow::Result<Vec<_>>>()?
                    .into_iter()
                    .sum::<usize>();
                let response = query(&client, &original.space, json!({"query":original.query,
                    "ranking_mode":"semantic","limit":corpus.metric.cutoff,"source_revision_constraints":constraints})).await?;
                let after = endpoints
                    .iter()
                    .map(spy::Spy::count)
                    .collect::<anyhow::Result<Vec<_>>>()?
                    .into_iter()
                    .sum::<usize>();
                anyhow::ensure!(
                    after == before + 1 && response.results.len() == corpus.metric.cutoff,
                    "negative control did not actually dispatch and retrieve indexed candidates"
                );
                anyhow::ensure!(!response.partial && response.coverage.len() == constraints.len());
                for (external, revision) in &constraints {
                    let coverage = response
                        .coverage
                        .get(external)
                        .context("current inventory coverage missing")?;
                    anyhow::ensure!(
                        coverage.state == "complete"
                            && coverage.requested_revision.as_ref() == Some(revision)
                            && coverage.desired_revision.as_ref() == Some(revision)
                            && coverage.published_revision.as_ref() == Some(revision)
                    );
                }
                let expected: Vec<_> = corpus
                    .evidence
                    .iter()
                    .filter(|passage| {
                        passage.space == original.space && original.required.contains(&passage.id)
                    })
                    .collect();
                let mut seen = HashSet::new();
                let mut supported = HashSet::new();
                let mut first_relevant = None;
                let mut rank = 0;
                for result in &response.results {
                    anyhow::ensure!(matches!(result.kind, SearchKind::Chunk));
                    for reference in &result.evidence {
                        let source = corpus
                            .sources
                            .iter()
                            .find(|source| {
                                source.space == original.space
                                    && source.id == reference.source_id
                                    && source.revision == reference.revision
                            })
                            .context("foreign source returned")?;
                        anyhow::ensure!(source.state.as_deref() != Some("superseded"));
                        anyhow::ensure!(
                            source
                                .text
                                .get(reference.span.start as usize..reference.span.end as usize)
                                == Some(reference.quote.as_str())
                        );
                        anyhow::ensure!(result.text == reference.quote);
                        if seen.insert((&reference.source_id, &reference.revision)) {
                            rank += 1;
                            for passage in &expected {
                                if reference.source_id == passage.source_id
                                    && reference.revision == passage.revision
                                    && reference.span.start <= passage.start
                                    && reference.span.end >= passage.end
                                {
                                    first_relevant.get_or_insert(rank);
                                    if rank <= corpus.metric.cutoff {
                                        supported.insert(passage.id.clone());
                                    }
                                }
                            }
                        }
                    }
                }
                if !expected.is_empty() {
                    positive_queries += 1;
                    reciprocal_sum += first_relevant.map_or(0.0, |rank| 1.0 / rank as f64);
                    all_required &= supported.len() == expected.len();
                }
                cases.push(
                    json!({"id":original.id,"query":original.query,"space":original.space,
                    "fallback_fetches":0,"response":response}),
                );
            }
            anyhow::ensure!(positive_queries > 0);
            let mrr = reciprocal_sum / f64::from(positive_queries);
            anyhow::ensure!(!all_required && mrr < corpus.metric.min_macro_reciprocal_rank,
                "{backend:?}/seed={seed}: valid random vectors met semantic expectations (MRR={mrr}); retain failure, never lower threshold");
            eprintln!("INDEXED NEGATIVE CONTROL ONLY: backend={backend:?} seed={seed} mrr={mrr} all_required={all_required} product_acceptance=false");
            // Optional capture goes to an explicitly selected qualification dir.
            // It contains the actual shared public engine responses, labelled
            // ineligible for provider acceptance, for D's separate Python oracle.
            if let Some(directory) = std::env::var_os("SEMANTIC_CONTROL_CAPTURE_DIR") {
                let path = std::path::PathBuf::from(directory);
                tokio::fs::create_dir_all(&path).await?;
                let capture = json!({"manifest":{"evidence_class":"indexed-fake-vector-negative-control",
                    "generator":"sha256-counter-semantic-index-negative-v1","seed":seed,
                    "query_mode":"semantic-only","fallback_fetches":0,"query_rewrites":0,
                    "authentication_enabled":false,"authorization_context":"fixed in-process principal with production ABAC",
                    "corpus_sha256":"fa6a84149483d3511fd559ac9a13e89d6c1c041ae97d6f15084b52b7d7f21c1b",
                    "backend":format!("{backend:?}"),"product_acceptance":false},"cases":cases});
                tokio::fs::write(
                    path.join(format!("{backend:?}-{seed}.json")),
                    serde_json::to_vec_pretty(&capture)?,
                )
                .await?;
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn query_vector_reuse_keeps_fresh_results_after_physical_source_erasure() -> anyhow::Result<()>
{
    use elephant_storage::canonical_commit::CanonicalRepository;
    use elephant_storage::source_publication::{SourceMutation, SourcePublicationRepository};
    use elephant_storage::SourceRepository;
    use elephant_types::publication::{OperationState, SourcePrecondition};
    use elephant_types::records::SourceType;

    for backend in BACKENDS {
        let client = spy::storage(backend).await?;
        let config = SpaceConfigRepository::new(client.clone());
        let endpoint = spy::Spy::start().await?;
        let space = SPACES[0];
        spy::configure(&config, space, &endpoint.endpoint, 8).await?;
        let original = "supply location";
        let erased_text = "The supply valve is in the utility cupboard.";
        let survivor_text = "The replacement supply valve is beside the back door.";
        endpoint.declared_vectors(BTreeMap::from([
            (original.into(), direction(1.0, 0.0)),
            (erased_text.into(), direction(1.0, 0.0)),
            (survivor_text.into(), direction(0.8, 0.6)),
        ]))?;
        for (external, text) in [
            ("erase-query-source", erased_text),
            ("keep-query-source", survivor_text),
        ] {
            fixture::publish(
                &client,
                space,
                external,
                "r1",
                text,
                fixture::security("index-a", 0),
                &[],
            )
            .await?;
        }
        let before = endpoint.count()?;
        let semantic = query(
            &client,
            space,
            json!({"query":original,"ranking_mode":"semantic"}),
        )
        .await?;
        anyhow::ensure!(semantic.results.len() == 2);
        anyhow::ensure!(semantic.results[0].text == erased_text);
        let hybrid = query(
            &client,
            space,
            json!({"query":original,"ranking_mode":"hybrid"}),
        )
        .await?;
        anyhow::ensure!(hybrid
            .results
            .iter()
            .any(|result| result.text == erased_text));
        anyhow::ensure!(
            endpoint.count()? == before + 1,
            "exact semantic/hybrid query was dispatched twice"
        );

        let publications = SourcePublicationRepository::new(client.clone());
        let head = publications
            .external_head(space, SourceType::Manual, "erase-query-source")
            .await?
            .context("published source head")?;
        let source = head.source_id.clone();
        let epoch = CanonicalRepository::new(client.clone())
            .observe_epoch(space)
            .await?;
        let receipt = Box::pin(
            publications.erase_live(SourceMutation {
                space_id: space.into(),
                source_id: source.clone(),
                expected: SourcePrecondition::Generation(head.generation),
                idempotency_key: "query-reuse-physical-erasure".into(),
                request_fingerprint: "query-reuse-physical-erasure".into(),
                actor: "query-vector-control".into(),
                authorized: publications
                    .authorization_snapshot(space, &source)
                    .await?
                    .context("source authorization")?,
                authorized_document: None,
            }),
        )
        .await?;
        anyhow::ensure!(receipt.state == OperationState::Erased);
        anyhow::ensure!(
            SourceRepository::new(client.clone())
                .get_with_deleted(space, &source)
                .await?
                .is_none(),
            "source row was not physically removed"
        );
        anyhow::ensure!(
            CanonicalRepository::new(client.clone())
                .observe_epoch(space)
                .await?
                != epoch
        );
        let after_erasure = endpoint.count()?;
        for mode in ["semantic", "hybrid"] {
            let result = query(
                &client,
                space,
                json!({"query":original,"ranking_mode":mode}),
            )
            .await?;
            anyhow::ensure!(!result.results.is_empty());
            anyhow::ensure!(result
                .results
                .iter()
                .all(|result| result.text != erased_text
                    && result
                        .evidence
                        .iter()
                        .all(|evidence| evidence.source_id != "erase-query-source")));
            anyhow::ensure!(result
                .results
                .iter()
                .any(|result| result.text == survivor_text));
        }
        anyhow::ensure!(
            endpoint.count()? == after_erasure,
            "fresh corpus reads reuse only the independently supplied caller vector"
        );
        spy::edit(&config, space, |processing| {
            processing
                .bindings
                .get_mut(&ModelOperation::QueryEmbedding)
                .context("query binding")?
                .revoked = true;
            Ok(())
        })
        .await?;
        let before_refusal = endpoint.count()?;
        anyhow::ensure!(query(
            &client,
            space,
            json!({"query":original,"ranking_mode":"semantic"})
        )
        .await
        .is_err());
        anyhow::ensure!(endpoint.count()? == before_refusal);
    }
    Ok(())
}
