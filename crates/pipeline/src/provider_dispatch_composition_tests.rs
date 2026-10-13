//! Closed-world ordering/accounting controls. No model or transport is invoked.

use super::*;
use elephant_embeddings::governed::ProviderDispatchObserver;
use elephant_embeddings::{EmbeddingError, EmbeddingResult};
use elephant_types::provider_binding::SpaceConfigToken;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Mutex,
};

#[derive(Debug, PartialEq, Eq)]
enum Event {
    Before(&'static str, String, ModelBindingProvenance),
    Usage(String, ModelBindingProvenance, Option<u64>, Option<u64>),
    Finished(String, ModelBindingProvenance),
}

#[derive(Debug)]
struct Observer {
    name: &'static str,
    events: Arc<Mutex<Vec<Event>>>,
    refuse: bool,
    // The guard reads the state changed while original durable admission waits.
    current: Option<Arc<AtomicBool>>,
    entered: Option<Arc<tokio::sync::Notify>>,
    release: Option<Arc<tokio::sync::Notify>>,
}

#[async_trait::async_trait]
impl ProviderDispatchObserver for Observer {
    #[expect(
        clippy::unwrap_used,
        reason = "A poisoned fixture event recorder must fail this ordering control at its original observation point."
    )]
    async fn before_dispatch(&self, id: &str, p: &ModelBindingProvenance) -> EmbeddingResult<()> {
        self.events
            .lock()
            .unwrap()
            .push(Event::Before(self.name, id.into(), p.clone()));
        if let Some(entered) = &self.entered {
            entered.notify_one();
        }
        if let Some(release) = &self.release {
            release.notified().await;
        }
        if self.refuse
            || self
                .current
                .as_ref()
                .is_some_and(|v| !v.load(Ordering::SeqCst))
        {
            return Err(EmbeddingError::ProviderError(format!(
                "{} refused",
                self.name
            )));
        }
        Ok(())
    }

    #[expect(
        clippy::unwrap_used,
        reason = "A poisoned fixture event recorder must fail the original usage-ownership observation."
    )]
    async fn observe_usage(
        &self,
        id: &str,
        p: &ModelBindingProvenance,
        input: Option<u64>,
        output: Option<u64>,
    ) -> EmbeddingResult<()> {
        assert_eq!(
            self.name, "owner",
            "additional guards cannot own measured usage"
        );
        self.events
            .lock()
            .unwrap()
            .push(Event::Usage(id.into(), p.clone(), input, output));
        Ok(())
    }

    #[expect(
        clippy::unwrap_used,
        reason = "A poisoned fixture event recorder must fail the original completion-ownership observation."
    )]
    async fn dispatch_finished(&self, id: &str, p: &ModelBindingProvenance) -> EmbeddingResult<()> {
        assert_eq!(
            self.name, "owner",
            "additional guards cannot own completion"
        );
        self.events
            .lock()
            .unwrap()
            .push(Event::Finished(id.into(), p.clone()));
        Ok(())
    }
}

fn observer(name: &'static str, events: &Arc<Mutex<Vec<Event>>>) -> Observer {
    Observer {
        name,
        events: events.clone(),
        refuse: false,
        current: None,
        entered: None,
        release: None,
    }
}

fn provenance() -> ModelBindingProvenance {
    ModelBindingProvenance {
        config: SpaceConfigToken {
            space_id: "named-space".into(),
            config_id: "space_config:original".into(),
            generation: 7,
            policy_id: "original-policy".into(),
        },
        operation: ModelOperation::Summary,
        binding_id: "original-binding".into(),
        profile_id: "original-profile".into(),
    }
}

#[tokio::test]
#[expect(
    clippy::unwrap_used,
    reason = "Fixture recorder or task failure must fail this control while preserving its original wait and revocation ordering."
)]
async fn guard_observes_revocation_during_original_durable_wait() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let current = Arc::new(AtomicBool::new(true));
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let mut owner = observer("owner", &events);
    owner.entered = Some(entered.clone());
    owner.release = Some(release.clone());
    let mut guard = observer("guard", &events);
    guard.current = Some(current.clone());
    let composed = AdditionalDispatchGuard {
        original: Some(Arc::new(owner)),
        guard: Arc::new(guard),
    };
    let p = provenance();
    let request = tokio::spawn(async move { composed.before_dispatch("original-call", &p).await });
    entered.notified().await;
    assert_eq!(
        events.lock().unwrap().len(),
        1,
        "guard must wait for durable admission"
    );
    current.store(false, Ordering::SeqCst);
    release.notify_one();
    assert!(request.await.unwrap().is_err());
    assert_eq!(
        *events.lock().unwrap(),
        vec![
            Event::Before("owner", "original-call".into(), provenance()),
            Event::Before("guard", "original-call".into(), provenance()),
        ],
        "refusal must not forge usage or completion"
    );
}

#[tokio::test]
#[expect(
    clippy::unwrap_used,
    reason = "Unexpected fixture dispatch or recorder failure must fail the original ordered receipt assertions."
)]
async fn composed_guards_preserve_order_and_only_original_receipts() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let first = AdditionalDispatchGuard {
        original: Some(Arc::new(observer("owner", &events))),
        guard: Arc::new(observer("first", &events)),
    };
    let second = AdditionalDispatchGuard {
        original: Some(Arc::new(first)),
        guard: Arc::new(observer("second", &events)),
    };
    let p = provenance();
    second.before_dispatch("physical-call", &p).await.unwrap();
    second
        .observe_usage("physical-call", &p, Some(17), None)
        .await
        .unwrap();
    second.dispatch_finished("physical-call", &p).await.unwrap();
    assert_eq!(
        *events.lock().unwrap(),
        vec![
            Event::Before("owner", "physical-call".into(), p.clone()),
            Event::Before("first", "physical-call".into(), p.clone()),
            Event::Before("second", "physical-call".into(), p.clone()),
            Event::Usage("physical-call".into(), p.clone(), Some(17), None),
            Event::Finished("physical-call".into(), p),
        ]
    );
}

#[tokio::test]
#[expect(
    clippy::unwrap_used,
    reason = "A poisoned fixture recorder must fail the original short-circuit event assertion."
)]
async fn original_refusal_short_circuits_all_added_guards() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let mut owner = observer("owner", &events);
    owner.refuse = true;
    let composed = AdditionalDispatchGuard {
        original: Some(Arc::new(owner)),
        guard: Arc::new(observer("guard", &events)),
    };
    assert!(composed
        .before_dispatch("refused", &provenance())
        .await
        .is_err());
    assert_eq!(
        *events.lock().unwrap(),
        vec![Event::Before("owner", "refused".into(), provenance())]
    );
}

#[tokio::test]
#[expect(
    clippy::unwrap_used,
    reason = "Unexpected fixture dispatch or recorder failure must fail the original no-accounting ownership control."
)]
async fn guard_without_accounting_does_not_become_receipt_owner() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let composed = AdditionalDispatchGuard {
        original: None,
        guard: Arc::new(observer("guard", &events)),
    };
    let p = provenance();
    composed.before_dispatch("unobserved", &p).await.unwrap();
    composed
        .observe_usage("unobserved", &p, None, Some(4))
        .await
        .unwrap();
    composed.dispatch_finished("unobserved", &p).await.unwrap();
    assert_eq!(
        *events.lock().unwrap(),
        vec![Event::Before("guard", "unobserved".into(), p)]
    );
}
