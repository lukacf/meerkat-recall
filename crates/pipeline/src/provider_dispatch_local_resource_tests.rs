//! Opt-in installed-model regression through fresh production dispatch contexts.
//! Requires an explicitly provisioned bundle; never downloads or falls back.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use super::*;
use elephant_embeddings::governed::ProviderDispatchObserver;
use elephant_embeddings::EmbeddingResult;
use elephant_storage::{Repository, StorageClient};
use elephant_types::provider_binding::SpaceProcessingConfig;
use elephant_types::records::{base::BaseFields, SpaceConfig};
use elephant_types::{RecordId, TableName};
use std::sync::Mutex;

#[derive(Clone, Debug)]
struct ObservedUsage {
    call_id: String,
    provenance: ModelBindingProvenance,
    input_tokens: Option<u64>,
    output_tokens: Option<u64>,
}

#[derive(Debug, Default)]
struct RequestObservation {
    admitted: Mutex<Vec<String>>,
    finished: Mutex<Vec<String>>,
    usage: Mutex<Vec<ObservedUsage>>,
}

#[async_trait::async_trait]
impl ProviderDispatchObserver for RequestObservation {
    async fn before_dispatch(&self, id: &str, _: &ModelBindingProvenance) -> EmbeddingResult<()> {
        self.admitted.lock().unwrap().push(id.to_owned());
        Ok(())
    }
    async fn observe_usage(
        &self,
        call_id: &str,
        provenance: &ModelBindingProvenance,
        input: Option<u64>,
        output: Option<u64>,
    ) -> EmbeddingResult<()> {
        self.usage.lock().unwrap().push(ObservedUsage {
            call_id: call_id.to_owned(),
            provenance: provenance.clone(),
            input_tokens: input,
            output_tokens: output,
        });
        Ok(())
    }
    async fn dispatch_finished(&self, id: &str, _: &ModelBindingProvenance) -> EmbeddingResult<()> {
        self.finished.lock().unwrap().push(id.to_owned());
        Ok(())
    }
}

async fn installed_fixture() -> (StorageClient, SpaceConfigRepository, u64) {
    let endpoint = std::env::var("RECALL_TEST_FASTEMBED_ENDPOINT").expect("explicit file endpoint");
    let digest =
        std::env::var("RECALL_TEST_FASTEMBED_MANIFEST_SHA256").expect("exact manifest digest");
    let root = reqwest::Url::parse(&endpoint)
        .unwrap()
        .to_file_path()
        .unwrap();
    let manifest: serde_json::Value =
        serde_json::from_slice(&std::fs::read(root.join(&digest).join("manifest.json")).unwrap())
            .unwrap();
    let dimensions = manifest["dimensions"].as_u64().unwrap();
    let processing: SpaceProcessingConfig = serde_json::from_value(serde_json::json!({
        "generation":1, "index_generation":0, "policy_id":"local-resource-fixture",
        "connections":{"local":{"id":"local","provider":"fastembed","account_id":"fixture",
            "endpoint":endpoint,"credential_ref":null,"credential_generation":1,"revoked":false}},
        "profiles":{"model":{"id":"model","connection_id":"local","model":manifest["model"],
            "model_version":format!("sha256:{digest}"),"dimensions":dimensions,
            "preprocessing":manifest["preprocessing"]}},
        "bindings":{"query_embedding":{"id":"query","profile_id":"model","revoked":false}},
        "active_embedding_profile_id":"model","pending_reindex":null
    }))
    .unwrap();
    let storage = StorageClient::new_embedded().await.unwrap();
    let repository = SpaceConfigRepository::new(storage.clone());
    repository
        .create(SpaceConfig {
            base: BaseFields::new(
                RecordId::new_v4(TableName::SpaceConfig),
                "fresh-local".into(),
                "test".into(),
            ),
            embedding_provider: "fastembed".into(),
            embedding_model: manifest["model"].as_str().unwrap().into(),
            embedding_dims: i32::try_from(dimensions).unwrap(),
            processing: Some(processing),
            purpose: None,
            skill: None,
        })
        .await
        .unwrap();
    (storage, repository, dimensions)
}

#[tokio::test]
#[ignore = "requires explicitly provisioned approved local model"]
async fn fresh_contexts_reuse_actual_model_constructor_and_keep_request_receipts_separate() {
    let (storage, repository, dimensions) = installed_fixture().await;
    let resources = storage.local_models();
    let mut observations = Vec::new();
    for _ in 0..3 {
        let observer = Arc::new(RequestObservation::default());
        // Fresh repository, context and governed provider on every iteration.
        let context = ProviderDispatchContext::new(
            SpaceConfigRepository::new(storage.clone()),
            "fresh-local",
        )
        .with_observer(observer.clone());
        let (_, provider, provenance) = context
            .embedding(ModelOperation::QueryEmbedding)
            .await
            .unwrap();
        let vectors = provider
            .embed(&["A finite local resource fixture.".into()])
            .await
            .unwrap();
        assert_eq!(vectors.len(), 1);
        assert_eq!(vectors[0].len(), usize::try_from(dimensions).unwrap());
        assert_eq!(
            resources.successful_initializations(),
            1,
            "actual verified-model constructor must run once across fresh handles"
        );
        observations.push((observer, provenance));
    }
    let mut calls = std::collections::BTreeSet::new();
    for (observation, provenance) in observations {
        let admitted = observation.admitted.lock().unwrap().clone();
        assert_eq!(admitted.len(), 1);
        assert_eq!(admitted, *observation.finished.lock().unwrap());
        let usage = observation.usage.lock().unwrap().clone();
        assert_eq!(usage.len(), 1);
        assert_eq!(usage[0].call_id, admitted[0]);
        assert_eq!(usage[0].provenance, provenance);
        assert!(usage[0].input_tokens.is_some_and(|count| count > 0));
        assert_eq!(usage[0].output_tokens, Some(0));
        assert!(
            calls.insert(admitted[0].clone()),
            "each request keeps its own receipt"
        );
    }
    let (_, stale, _) = ProviderDispatchContext::new(repository.clone(), "fresh-local")
        .embedding(ModelOperation::QueryEmbedding)
        .await
        .unwrap();
    let original = repository
        .get_by_space_id("fresh-local")
        .await
        .unwrap()
        .unwrap();
    let mut revoked = original.clone();
    let config = revoked.processing.as_mut().unwrap();
    config.generation += 1;
    config
        .bindings
        .get_mut(&ModelOperation::QueryEmbedding)
        .unwrap()
        .revoked = true;
    repository
        .replace_authorized(&original, revoked)
        .await
        .unwrap();
    assert!(stale
        .embed(&["Must not infer after revocation.".into()])
        .await
        .is_err());
    assert_eq!(resources.successful_initializations(), 1);
}
