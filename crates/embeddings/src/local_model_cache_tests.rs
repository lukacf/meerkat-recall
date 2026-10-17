//! Deterministic constructor and executing-owner tests. No ONNX or model probes.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::{oneshot, OwnedSemaphorePermit};

const DEADLINE: Duration = Duration::from_secs(5);

#[cfg(feature = "fastembed")]
#[tokio::test]
async fn bge_retrieval_cache_shares_resources_across_roles_but_separates_recipe_identity() {
    use elephant_types::provider_binding::*;
    let mut binding = ResolvedModelBinding {
        provenance: ModelBindingProvenance {
            config: SpaceConfigToken {
                space_id: "test".into(),
                config_id: "cfg".into(),
                generation: 1,
                policy_id: "test".into(),
            },
            operation: ModelOperation::QueryEmbedding,
            binding_id: "query".into(),
            profile_id: "profile".into(),
        },
        connection: ProviderConnection {
            id: "assets".into(),
            provider: "fastembed".into(),
            account_id: "local".into(),
            endpoint: "file:///approved-assets".into(),
            credential_ref: None,
            credential_generation: 1,
            revoked: false,
        },
        profile: ModelProfile {
            id: "profile".into(),
            connection_id: "assets".into(),
            model: "bge-small-en-v1.5".into(),
            model_version: format!("sha256:{}", "a".repeat(64)),
            dimensions: Some(384),
            preprocessing: BGE_EN_RETRIEVAL_V1.into(),
            summary: None,
        },
    };
    let slot = ResourceSlot::default();
    let loads = Arc::new(AtomicUsize::new(0));
    let query = slot
        .get_or_try_init(LocalModelIdentity::from(&binding), loader(1, loads.clone()))
        .await
        .unwrap();
    binding.provenance.operation = ModelOperation::DocumentEmbedding;
    binding.provenance.binding_id = "document".into();
    let document = slot
        .get_or_try_init(
            LocalModelIdentity::from(&binding),
            loader(99, loads.clone()),
        )
        .await
        .unwrap();
    assert!(Arc::ptr_eq(&query, &document));
    assert_eq!(loads.load(Ordering::SeqCst), 1);
    drop(query);
    drop(document);
    let original_key = LocalModelIdentity::from(&binding);
    binding.profile.preprocessing = "text-v1".into();
    assert!(original_key != LocalModelIdentity::from(&binding));
    // Different manifest is also part of the key, independently of recipe.
    let previous_key = LocalModelIdentity::from(&binding);
    binding.profile.model_version = format!("sha256:{}", "b".repeat(64));
    assert!(previous_key != LocalModelIdentity::from(&binding));
    let replacement = tokio::time::timeout(
        DEADLINE,
        slot.get_or_try_init(LocalModelIdentity::from(&binding), loader(2, loads.clone())),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(replacement.identity, 2);
    drop(replacement);
    assert_eq!(loads.load(Ordering::SeqCst), 2);
}

#[derive(Debug)]
struct FixtureModel {
    identity: usize,
    _capacity: OwnedSemaphorePermit,
}

fn loader(
    identity: usize,
    loads: Arc<AtomicUsize>,
) -> impl FnOnce(OwnedSemaphorePermit) -> EmbeddingResult<Arc<FixtureModel>> + Send {
    move |capacity| {
        loads.fetch_add(1, Ordering::SeqCst);
        Ok(Arc::new(FixtureModel {
            identity,
            _capacity: capacity,
        }))
    }
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Stage every concurrent request before joining and observe explicit model and service-owner drops."
)]
async fn fresh_requesters_coalesce_one_constructor_and_release_with_owner() {
    let slot = Arc::new(ResourceSlot::default());
    let loads = Arc::new(AtomicUsize::new(0));
    let mut requests = Vec::new();
    for _ in 0..24 {
        let request_slot = slot.clone();
        let load = loader(7, loads.clone());
        requests.push(tokio::spawn(async move {
            request_slot
                .get_or_try_init("manifest-a", load)
                .await
                .unwrap()
        }));
    }
    let mut models = Vec::new();
    for request in requests {
        models.push(
            tokio::time::timeout(DEADLINE, request)
                .await
                .unwrap()
                .unwrap(),
        );
    }
    assert_eq!(loads.load(Ordering::SeqCst), 1);
    assert!(models.iter().all(|model| Arc::ptr_eq(model, &models[0])));
    let weak = Arc::downgrade(&models[0]);
    drop(models);
    assert!(weak.upgrade().is_some(), "service retains its warmed model");
    drop(slot);
    assert!(weak.upgrade().is_none(), "no process-global resource owner");
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep slot and replacement owners alive across the eviction, blocked-load and release assertions."
)]
async fn changed_identity_waits_for_actual_executing_owner_before_replacement() {
    let slot = Arc::new(ResourceSlot::default());
    let loads = Arc::new(AtomicUsize::new(0));
    let first = slot
        .get_or_try_init("a", loader(1, loads.clone()))
        .await
        .unwrap();
    let old = Arc::downgrade(&first);
    let request_slot = slot.clone();
    let load = loader(2, loads.clone());
    let second = tokio::spawn(async move { request_slot.get_or_try_init("b", load).await });
    // Observe eviction while the executing owner still holds capacity. This
    // uses the semaphore queue, not timing as the synchronization condition.
    tokio::time::timeout(DEADLINE, async {
        while Arc::strong_count(&first) != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(loads.load(Ordering::SeqCst), 1);
    assert!(!second.is_finished());
    drop(first);
    let second = tokio::time::timeout(DEADLINE, second)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(second.identity, 2);
    assert_eq!(loads.load(Ordering::SeqCst), 2);
    assert!(old.upgrade().is_none());
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep service and retry owners across the original start, cancellation, poll and release barriers."
)]
async fn cancelled_initialization_keeps_its_slot_until_constructor_finishes() {
    let slot = Arc::new(ResourceSlot::default());
    let loads = Arc::new(AtomicUsize::new(0));
    let (started, started_rx) = oneshot::channel();
    let (release, release_rx) = std::sync::mpsc::channel();
    let request_slot = slot.clone();
    let load_count = loads.clone();
    let original = tokio::spawn(async move {
        request_slot
            .get_or_try_init("a", move |capacity| {
                load_count.fetch_add(1, Ordering::SeqCst);
                started.send(()).unwrap();
                release_rx.recv_timeout(DEADLINE).unwrap();
                Ok(Arc::new(FixtureModel {
                    identity: 1,
                    _capacity: capacity,
                }))
            })
            .await
    });
    tokio::time::timeout(DEADLINE, started_rx)
        .await
        .unwrap()
        .unwrap();
    original.abort();
    assert!(original.await.unwrap_err().is_cancelled());
    let mut retry = Box::pin(slot.get_or_try_init("a", loader(99, loads.clone())));
    // Poll the next requester while the cancelled request's constructor is
    // still held. It must wait for that original initialization, not start one.
    std::future::poll_fn(|context| {
        assert!(std::future::Future::poll(retry.as_mut(), context).is_pending());
        std::task::Poll::Ready(())
    })
    .await;
    assert_eq!(loads.load(Ordering::SeqCst), 1);
    release.send(()).unwrap();
    let reused = tokio::time::timeout(DEADLINE, retry)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(reused.identity, 1);
    assert_eq!(loads.load(Ordering::SeqCst), 1);
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "The same slot must survive the failed initialization and the successful replacement assertions."
)]
async fn failed_constructor_releases_capacity_and_does_not_poison_identity() {
    let slot = ResourceSlot::default();
    let failed: EmbeddingResult<Arc<FixtureModel>> = slot
        .get_or_try_init("a", |_capacity| {
            Err(EmbeddingError::ProviderError(
                "fixture rejected assets".into(),
            ))
        })
        .await;
    assert!(failed.is_err());
    let loads = Arc::new(AtomicUsize::new(0));
    let retry = tokio::time::timeout(
        DEADLINE,
        slot.get_or_try_init("a", loader(1, loads.clone())),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(retry.identity, 1);
    assert_eq!(loads.load(Ordering::SeqCst), 1);
}
