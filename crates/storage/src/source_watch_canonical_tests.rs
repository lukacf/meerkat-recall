//! Real queued Watch ancestry through canonical maintenance, without providers.
use super::*;
use crate::canonical_commit::{
    CanonicalCommit, CanonicalEffect, CanonicalMaintenanceContext, CanonicalMaintenanceOrigin,
    CanonicalPlan, CanonicalRecord, CanonicalRepository,
};
use crate::{DocRepository, Repository, SourceRepository};
use elephant_types::records::Doc;

#[derive(Clone, Copy, Debug)]
enum DeniedInput {
    ProposedDocument,
    OriginalDocument,
    CurrentSource,
    RevokedWatch,
}

fn document(source: &RecordId) -> Doc {
    Doc {
        base: base(TableName::Doc),
        title: "original queued maintenance document".into(),
        text: "original queued maintenance document".into(),
        sidecar_text: None,
        source: Some(Link::new(source.clone())),
        media_attachments: Vec::new(),
        rendition_key: "standing-canonical".into(),
        metadata: serde_json::json!({}),
    }
}

async fn request(f: &Fixture, commit_id: &str, before: &Doc, after: &Doc) -> CanonicalCommit {
    CanonicalCommit {
        space_id: "watch-space".into(),
        commit_id: commit_id.into(),
        input_fingerprint: commit_id.into(),
        replay_output: None,
        publication: None,
        maintenance: Some(CanonicalMaintenanceContext {
            origin: CanonicalMaintenanceOrigin::IndependentMutation,
            attempt: Some(f.producer.original_attempt.clone()),
            sources: Vec::new(),
        }),
        finalization: None,
        plan: CanonicalPlan {
            observed_epoch: CanonicalRepository::new(f.client.clone())
                .epoch("watch-space")
                .await
                .unwrap(),
            dependencies: vec![CanonicalRecord::Doc(Box::new(before.clone()))],
            effects: vec![CanonicalEffect {
                before: Some(CanonicalRecord::Doc(Box::new(before.clone()))),
                after: Some(CanonicalRecord::Doc(Box::new(after.clone()))),
            }],
            extension_effects: Vec::new(),
            outbox: Vec::new(),
        },
    }
}

#[expect(
    clippy::large_futures,
    reason = "Keep the conformance helper's original await and allocation shape."
)]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture, repository, and mutation owners alive through the original conformance phase."
)]
async fn original_watch_ceilings_fence_canonical_inputs(embedded: bool, denied: DeniedInput) {
    let f = fixture(embedded).await;
    if !embedded {
        f.client
            .query_checked(include_str!(
                "../../migrations/sql/043_native_read_fence.surql"
            ))
            .await
            .unwrap();
    }
    let docs = DocRepository::new(f.client.clone());
    let before = docs
        .create(document(&f.request.source.base.id))
        .await
        .unwrap();
    let mut allowed = before.clone();
    allowed.text = "authorized original queued write".into();
    let canonical = CanonicalRepository::new(f.client.clone());
    let positive = request(&f, "standing-canonical-positive", &before, &allowed).await;
    canonical.commit(positive).await.unwrap();
    let mut current = docs
        .get("watch-space", &before.base.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(current.text, allowed.text);
    let mut proposed = current.clone();
    proposed.text = "must never become visible".into();
    match denied {
        DeniedInput::ProposedDocument => proposed.base.security.level = 1,
        DeniedInput::OriginalDocument => {
            current.base.security.level = 1;
            current = docs
                .update("watch-space", &current.base.id.clone(), current)
                .await
                .unwrap();
            // An allowed after-image cannot hide an unauthorized original read.
            proposed.base.security.level = 0;
        }
        DeniedInput::CurrentSource => {
            let sources = SourceRepository::new(f.client.clone());
            let mut source = sources
                .get("watch-space", &f.request.source.base.id)
                .await
                .unwrap()
                .unwrap();
            source.base.security.level = 1;
            sources
                .update("watch-space", &source.base.id.clone(), source)
                .await
                .unwrap();
        }
        DeniedInput::RevokedWatch => {
            let mut watch = f.producer.watch.clone();
            watch.status = SourceWatchStatus::Paused;
            crate::SourceWatchRepository::new(f.client.clone())
                .update("watch-space", &watch.base.id.clone(), watch)
                .await
                .unwrap();
        }
    }
    // Capture after the policy change: a stale epoch or mismatched original row
    // must not supply a false positive for the actual authority refusal.
    let negative = request(&f, "standing-canonical-denied", &current, &proposed).await;
    assert!(canonical.commit(negative).await.is_err(), "{denied:?}");
    assert_eq!(
        docs.get("watch-space", &current.base.id).await.unwrap(),
        Some(current)
    );
    assert!(canonical
        .receipt("watch-space", "standing-canonical-denied")
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
#[expect(
    clippy::large_futures,
    reason = "Keep the conformance helper's original await and allocation shape."
)]
async fn embedded_canonical_watch_checks_proposed_document() {
    original_watch_ceilings_fence_canonical_inputs(true, DeniedInput::ProposedDocument).await;
}
#[tokio::test]
#[expect(
    clippy::large_futures,
    reason = "Keep the conformance helper's original await and allocation shape."
)]
async fn surreal_canonical_watch_checks_proposed_document() {
    original_watch_ceilings_fence_canonical_inputs(false, DeniedInput::ProposedDocument).await;
}
#[tokio::test]
#[expect(
    clippy::large_futures,
    reason = "Keep the conformance helper's original await and allocation shape."
)]
async fn embedded_canonical_watch_checks_original_document() {
    original_watch_ceilings_fence_canonical_inputs(true, DeniedInput::OriginalDocument).await;
}
#[tokio::test]
#[expect(
    clippy::large_futures,
    reason = "Keep the conformance helper's original await and allocation shape."
)]
async fn surreal_canonical_watch_checks_original_document() {
    original_watch_ceilings_fence_canonical_inputs(false, DeniedInput::OriginalDocument).await;
}
#[tokio::test]
#[expect(
    clippy::large_futures,
    reason = "Keep the conformance helper's original await and allocation shape."
)]
async fn embedded_canonical_watch_checks_unlisted_current_source() {
    original_watch_ceilings_fence_canonical_inputs(true, DeniedInput::CurrentSource).await;
}
#[tokio::test]
#[expect(
    clippy::large_futures,
    reason = "Keep the conformance helper's original await and allocation shape."
)]
async fn surreal_canonical_watch_checks_unlisted_current_source() {
    original_watch_ceilings_fence_canonical_inputs(false, DeniedInput::CurrentSource).await;
}
#[tokio::test]
#[expect(
    clippy::large_futures,
    reason = "Keep the conformance helper's original await and allocation shape."
)]
async fn embedded_canonical_watch_checks_current_revocation() {
    original_watch_ceilings_fence_canonical_inputs(true, DeniedInput::RevokedWatch).await;
}
#[tokio::test]
#[expect(
    clippy::large_futures,
    reason = "Keep the conformance helper's original await and allocation shape."
)]
async fn surreal_canonical_watch_checks_current_revocation() {
    original_watch_ceilings_fence_canonical_inputs(false, DeniedInput::RevokedWatch).await;
}
