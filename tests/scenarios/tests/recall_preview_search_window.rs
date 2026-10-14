//! D-owned authorization/window oracle through actual indexed public search.
//! The peer-owned fixture supplies only transport and publication mechanics.
//! Declared numerical vectors are not evidence of real-provider relevance.

#[path = "../../recall_preview_semantics/index_fixture.rs"]
mod index_fixture;
#[path = "../../recall_preview/publication_support.rs"]
mod publication_support;
// This target consumes only the shared index spy, not its generation adapters.
#[allow(dead_code)]
#[path = "../../recall_preview_semantics/provider_spy.rs"]
mod spy;
#[path = "../../recall_preview/support.rs"]
mod support;

use elephant_mcp::{McpError, ToolDispatcher};
use elephant_pipeline::provider_dispatch::with_dispatch_credentials;
use elephant_storage::SpaceConfigRepository;
use elephant_types::public_error::ErrorCode;
use serde_json::{json, Value};
use std::{collections::BTreeMap, sync::Arc};

fn vector(x: f32, y: f32) -> Vec<f32> {
    let mut value = vec![0.0; 8];
    value[0] = x;
    value[1] = y;
    value
}

async fn search(client: &elephant_storage::StorageClient, query: &str) -> Result<Value, McpError> {
    let mut dispatcher = ToolDispatcher::new(index_fixture::reader(client, spy::SPACES[0]));
    dispatcher.register(elephant_mcp::scoped_search::ScopedSearchHandler);
    with_dispatch_credentials(
        Arc::new(spy::SpyCredentials),
        dispatcher.execute(
            "search_scoped",
            json!({
                "query":query, "ranking_mode":"semantic", "limit":1
            }),
        ),
    )
    .await
}

fn exact_visible_result(response: Value, target: &str) -> anyhow::Result<()> {
    let body = support::mcp_success_data(response)?;
    anyhow::ensure!(body["partial"] == false);
    let results = body["results"]
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("search results absent"))?;
    anyhow::ensure!(
        results.len() == 1,
        "physical window truncation became an empty success"
    );
    anyhow::ensure!(
        results[0]["kind"] == "chunk" && results[0]["text"] == target,
        "semantic index result was replaced by an ineligible hit or lexical rescue"
    );
    let evidence = results[0]["evidence"]
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("immutable evidence absent"))?;
    anyhow::ensure!(
        evidence.len() == 1
            && evidence[0]["source_id"] == "D-visible-window-target"
            && evidence[0]["revision"] == "target-r1"
            && evidence[0]["span"]["start"] == 0
            && evidence[0]["span"]["end"] == target.len()
            && evidence[0]["quote"] == target
    );
    anyhow::ensure!(
        body["coverage"].get("D-hidden-window-prefix").is_none(),
        "unauthorized source identity leaked through coverage"
    );
    Ok(())
}

#[tokio::test]
async fn ineligible_partition_prefix_cannot_become_complete_empty_semantic_search(
) -> anyhow::Result<()> {
    const QUERY: &str = "original D index window request";
    const TARGET: &str = "Close the brass valve inside the utility cupboard.";
    const HIDDEN_NODE: &str = "Restricted unrelated catalog entry.\n";
    // Frozen before any result: 401 higher-scoring hits exceed the initial
    // physical window while staying well below the source byte limit.
    const HIDDEN_NODES: usize = 401;
    for backend in spy::BACKENDS {
        let client = spy::storage(backend).await?;
        let endpoint = spy::Spy::start().await?;
        spy::configure(
            &SpaceConfigRepository::new(client.clone()),
            spy::SPACES[0],
            &endpoint.endpoint,
            8,
        )
        .await?;
        endpoint.declared_vectors(BTreeMap::from([
            (QUERY.into(), vector(1.0, 0.0)),
            (TARGET.into(), vector(0.8, 0.6)),
            (HIDDEN_NODE.into(), vector(1.0, 0.0)),
        ]))?;
        index_fixture::publish(
            &client,
            spy::SPACES[0],
            "D-visible-window-target",
            "target-r1",
            TARGET,
            index_fixture::security("default", 0),
            &[],
        )
        .await?;
        // Positive control proves the public profile, physical target and fixed
        // reader work before introducing the ineligible partition prefix.
        let before_cold_query = endpoint.count()?;
        exact_visible_result(search(&client, QUERY).await?, TARGET)?;
        anyhow::ensure!(
            endpoint.count()? == before_cold_query + 1,
            "the cold original query must dispatch exactly once"
        );
        let cold_requests = endpoint.requests()?;
        anyhow::ensure!(cold_requests[before_cold_query].body["input"] == json!([QUERY]));
        let text = HIDDEN_NODE.repeat(HIDDEN_NODES);
        let cuts: Vec<_> = (1..HIDDEN_NODES).map(|n| n * HIDDEN_NODE.len()).collect();
        let mut hidden = index_fixture::security("default", 0);
        hidden.labels = vec!["D-ungranted-label".into()];
        // Policy and level stay identical, so these hits occupy the same
        // accessible vector partition; only later row ABAC excludes them.
        index_fixture::publish(
            &client,
            spy::SPACES[0],
            "D-hidden-window-prefix",
            "hidden-r1",
            &text,
            hidden,
            &cuts,
        )
        .await?;
        let before = endpoint.count()?;
        match search(&client, QUERY).await {
            Ok(response) => exact_visible_result(response, TARGET)?,
            Err(McpError::Public(error)) if error.error_code == ErrorCode::RateLimited => {
                anyhow::ensure!(error.retryable, "bounded-work refusal lost retry policy");
            }
            Err(error) => return Err(error.into()),
        }
        anyhow::ensure!(
            // Same storage, caller, binding and literal query as the cold
            // control. Corpus writes change read authority, not the memoized
            // caller-input vector. No new call is needed to widen this window.
            endpoint.count()? == before,
            "window continuation rewrote or redispatched the original query"
        );
    }
    Ok(())
}
