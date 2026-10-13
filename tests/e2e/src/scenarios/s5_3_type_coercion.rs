#![allow(
    clippy::unwrap_used,
    clippy::mixed_attributes_style,
    clippy::similar_names,
    clippy::type_complexity,
    clippy::float_cmp,
    clippy::const_is_empty,
    clippy::expect_used,
    clippy::panic,
    unused_imports,
    dead_code,
    clippy::too_many_lines,
    clippy::cognitive_complexity
)]

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use elephant_mcp::{create_dispatcher, ToolResponse};
use elephant_pipeline::provider_dispatch::with_dispatch_credentials;
use elephant_policy::{Clearance, Principal};
use elephant_storage::SpaceConfigRepository;
use elephant_types::provider_binding::ModelOperation;

use crate::Harness;

// Controlled transport only: coercion exercises the retained default query
// route and its real approval checks, without a model-quality assertion.
#[expect(
    clippy::duplicate_mod,
    reason = "retain this scenario transport module and its existing independent type identity"
)]
#[path = "../../../recall_preview_semantics/provider_spy.rs"]
mod coercion_transport;

fn test_principal(space_id: &str) -> Principal {
    let mut scopes = HashSet::new();
    scopes.insert("*".to_string());

    Principal {
        space_id: space_id.to_string(),
        scopes,
        clearances: vec![Clearance {
            policy_id: "default".to_string(),
            max_level: i32::MAX,
            labels: HashSet::new(),
            handling_allow: HashSet::new(),
        }],
        subject_allowlist: None,
        purpose: HashSet::new(),
    }
}

#[tokio::test]
async fn scenario_s5_3_type_coercion() -> anyhow::Result<()> {
    let harness = Harness::new().await?;
    let dispatcher = create_dispatcher(harness.client.clone(), test_principal("default"));
    let spy = coercion_transport::Spy::start().await?;
    let config_repo = SpaceConfigRepository::new(harness.client.clone());
    coercion_transport::configure(&config_repo, "default", &spy.endpoint, 3).await?;
    coercion_transport::edit(&config_repo, "default", |processing| {
        processing
            .bindings
            .retain(|operation, _| *operation == ModelOperation::QueryEmbedding);
        Ok(())
    })
    .await?;
    spy.declared_vectors(BTreeMap::from([(
        "nonexistent".into(),
        vec![1.0, 0.0, 0.0],
    )]))?;

    // Verify schema-driven coercion: limit arrives as a string, schema expects integer.
    let params = serde_json::json!({
        "query": "nonexistent",
        "limit": "10"
    });

    let response = with_dispatch_credentials(
        Arc::new(coercion_transport::SpyCredentials),
        dispatcher.execute("search_assertions", params),
    )
    .await?;
    let decoded: ToolResponse<serde_json::Value> = serde_json::from_value(response)?;

    assert!(
        decoded.ok,
        "expected search_assertions ok=true (coercion), got error={:?} details={:?}",
        decoded.error, decoded.details
    );
    let requests = spy.requests()?;
    assert_eq!(requests.len(), 1, "one approved original-query embedding");
    assert_eq!(
        requests[0].body["input"],
        serde_json::json!(["nonexistent"])
    );

    Ok(())
}
