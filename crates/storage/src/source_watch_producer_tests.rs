//! Actual source acceptance and queue claims; no provider or network calls.
#![allow(clippy::expect_used, clippy::unwrap_used)]
use super::*;
use elephant_types::publication::SourcePrecondition;
use elephant_types::records::{BaseFields, Source, SourceType, SpaceConfig};
use elephant_types::Link;

fn base(table: TableName) -> BaseFields {
    BaseFields::new(
        RecordId::new_v4(table),
        "watch-space".into(),
        "watch-service".into(),
    )
}
struct Fixture {
    client: StorageClient,
    request: AcceptSourceRevision,
    producer: PreparedSourceWatchProducer,
}
#[expect(
    clippy::too_many_lines,
    reason = "Keep the complete fault and recovery sequence together with its original assertions."
)]
async fn fixture(embedded: bool) -> Fixture {
    let client = if embedded {
        StorageClient::new_embedded().await
    } else {
        StorageClient::new(crate::StorageConfig::default()).await
    }
    .unwrap();
    if !embedded {
        for sql in [
            include_str!("../../migrations/sql/031_source_publication.surql"),
            include_str!("../../migrations/sql/032_work_recovery.surql"),
            include_str!("../../migrations/sql/039_private_reference_fence.surql"),
            include_str!("../../migrations/sql/041_work_producer_origin.surql"),
            include_str!("../../migrations/sql/042_watch_delegation.surql"),
        ] {
            client.query_checked(sql).await.unwrap();
        }
    }
    crate::SpaceConfigRepository::new(client.clone())
        .create(SpaceConfig {
            base: base(TableName::SpaceConfig),
            embedding_provider: "fake".into(),
            embedding_model: "fixture".into(),
            embedding_dims: 8,
            processing: None,
            purpose: None,
            skill: None,
        })
        .await
        .unwrap();
    let repo = SourcePublicationRepository::new(client.clone());
    let source = Source::new(
        base(TableName::Source),
        SourceType::Web,
        "before-hash".into(),
    );
    let mut request = AcceptSourceRevision {
        source,
        revision_id: "watch-r1".into(),
        expected: SourcePrecondition::Absent,
        idempotency_key: "watch-first".into(),
        request_fingerprint: "watch-first-fingerprint".into(),
        reactivate: false,
        recreate_after_erasure: false,
        authorized: None,
        authorized_document: None,
    };
    let first = repo.accept(request.clone()).await.unwrap();
    request.source.hash = "after-hash".into();
    request.revision_id = "watch-r2".into();
    request.expected = SourcePrecondition::Generation(first.source_generation);
    request.idempotency_key = "watch-fetch-original".into();
    request.request_fingerprint = "watch-fetch-fingerprint".into();
    request.authorized = repo
        .authorization_snapshot("watch-space", &request.source.base.id)
        .await
        .unwrap();
    let watch = crate::SourceWatchRepository::new(client.clone())
        .create(SourceWatch {
            base: base(TableName::SourceWatch),
            source: Link::new(request.source.base.id.clone()),
            requested_uri: "https://example.invalid/watch".into(),
            status: SourceWatchStatus::Enabled,
            interval_seconds: 60,
            next_check_at: chrono::Utc::now(),
            last_checked_at: None,
            last_changed_at: None,
            etag: None,
            last_modified: None,
            observed_hash: None,
            consecutive_failures: 0,
            last_error: None,
            latest_check: None,
        })
        .await
        .unwrap();
    let delegation = register_local_delegation(&client, &watch).await;
    let queue = crate::WorkItemRepository::new(client.clone());
    let item = queue
        .create(WorkItem {
            base: base(TableName::WorkItem),
            kind: "refresh_source_watch".into(),
            payload: serde_json::json!({"watch_id":watch.base.id,"watch_intent_fingerprint":watch_intent_fingerprint(&watch,"watch-service").unwrap(),"standing_watch_delegation":delegation}),
            status: WorkItemStatus::Runnable,
            priority: 0,
            idempotency_key: "watch-original-queue-intent".into(),
            attempts: 0,
            lease_expires_at: None,
            leased_by: None,
            run_after: None,
            last_error: None,
        })
        .await
        .unwrap();
    let work = queue
        .claim_with_lease(
            "watch-space",
            &item.base.id,
            "watch-worker",
            chrono::Utc::now() + chrono::Duration::minutes(5),
        )
        .await
        .unwrap()
        .unwrap();
    let attempt = crate::work_attempt_repository::fence_from_claim(&work).unwrap();
    let mut run = WorkRun {
        base: base(TableName::WorkRun),
        work_item: Link::new(work.base.id.clone()),
        attempt: work.attempts,
        started_at: chrono::Utc::now(),
        completed_at: None,
        success: false,
        error: None,
        metrics: elephant_types::records::work::WorkRunMetrics::default(),
        worker_id: "watch-worker".into(),
        worker_version: "fixture".into(),
        model_version: None,
        prompt_id: None,
        config_hash: None,
    };
    run.metrics.custom.insert(
        "attempt_fence".into(),
        serde_json::to_value(&attempt).unwrap(),
    );
    let run = crate::WorkRunRepository::new(client.clone())
        .create(run)
        .await
        .unwrap();
    let producer = PreparedSourceWatchProducer::capture(
        &client,
        &watch,
        "watch-service",
        &attempt,
        &run.base.id,
    )
    .await
    .unwrap();
    Fixture {
        client,
        request,
        producer,
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
async fn original_watch_acceptance_and_historical_accounting(embedded: bool) {
    let f = fixture(embedded).await;
    let repo = SourcePublicationRepository::new(f.client.clone());
    let accepted = repo
        .accept_with_watch_producer(f.request.clone(), &f.producer, None, None)
        .await
        .unwrap();
    let fence = elephant_types::publication::PublicationFence {
        source_id: accepted.source_id.clone(),
        generation: accepted.source_generation.clone(),
        revision_id: f.request.revision_id.clone(),
        operation_id: accepted.operation_id.clone(),
    };
    let origin = repo
        .source_operation_producer("watch-space", &fence)
        .await
        .unwrap();
    assert_eq!(origin, f.producer.origin());
    assert!(matches!(origin, SourceProducerOrigin::Watch { .. }));
    // Historical accounting can inspect the original finished execution, but it
    // never grants a replacement live lease or changes this source's producer.
    let mut ended = f.producer.work.clone();
    ended.status = WorkItemStatus::Done;
    ended.leased_by = None;
    ended.lease_expires_at = None;
    assert!(verify_watch_origin(&origin, &ended, &f.producer.run).is_ok());
    let mut changed = ended.clone();
    changed.payload["watch_id"] = serde_json::json!(RecordId::new_v4(TableName::SourceWatch));
    assert!(verify_watch_origin(&origin, &changed, &f.producer.run).is_err());
    let mut wrong_run = f.producer.run.clone();
    wrong_run.metrics.custom.remove("attempt_fence");
    assert!(verify_watch_origin(&origin, &ended, &wrong_run).is_err());
    assert!(
        repo.accept(f.request).await.is_err(),
        "direct retry cannot launder Watch origin"
    );
}
#[expect(
    clippy::large_futures,
    reason = "Keep the conformance helper's original await and allocation shape."
)]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture, repository, and mutation owners alive through the original conformance phase."
)]
async fn changed_original_watch_or_attempt_cannot_accept(embedded: bool, change_watch: bool) {
    let f = fixture(embedded).await;
    let repo = SourcePublicationRepository::new(f.client.clone());
    let head = repo
        .head("watch-space", &f.request.source.base.id)
        .await
        .unwrap();
    if change_watch {
        let mut changed = f.producer.watch.clone();
        changed.status = SourceWatchStatus::Paused;
        crate::SourceWatchRepository::new(f.client.clone())
            .update("watch-space", &changed.base.id.clone(), changed)
            .await
            .unwrap();
    } else {
        let cancelled = crate::WorkItemRepository::new(f.client.clone())
            .update_running_for_worker(
                "watch-space",
                &f.producer.work.base.id,
                &f.producer.original_attempt.lease_token,
                WorkItemStatus::Cancelled,
                Some("original watch execution cancelled".into()),
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(cancelled.status, WorkItemStatus::Cancelled);
    }
    assert!(repo
        .accept_with_watch_producer(f.request.clone(), &f.producer, None, None)
        .await
        .is_err());
    assert_eq!(
        repo.head("watch-space", &f.request.source.base.id)
            .await
            .unwrap(),
        head
    );
    assert!(repo
        .revision(
            "watch-space",
            &f.request.source.base.id,
            &f.request.revision_id
        )
        .await
        .unwrap()
        .is_none());
    assert!(repo
        .matching_source_receipt(&f.request)
        .await
        .unwrap()
        .is_none());
}
#[tokio::test]
#[expect(
    clippy::large_futures,
    reason = "Keep the conformance helper's original await and allocation shape."
)]
async fn embedded_original_watch_producer_is_recorded_and_not_direct() {
    original_watch_acceptance_and_historical_accounting(true).await;
}
#[tokio::test]
#[expect(
    clippy::large_futures,
    reason = "Keep the conformance helper's original await and allocation shape."
)]
async fn surreal_original_watch_producer_is_recorded_and_not_direct() {
    original_watch_acceptance_and_historical_accounting(false).await;
}
#[tokio::test]
#[expect(
    clippy::large_futures,
    reason = "Keep the conformance helper's original await and allocation shape."
)]
async fn embedded_cancelled_watch_attempt_leaves_source_unchanged() {
    changed_original_watch_or_attempt_cannot_accept(true, false).await;
}
#[tokio::test]
#[expect(
    clippy::large_futures,
    reason = "Keep the conformance helper's original await and allocation shape."
)]
async fn surreal_cancelled_watch_attempt_leaves_source_unchanged() {
    changed_original_watch_or_attempt_cannot_accept(false, false).await;
}
#[tokio::test]
#[expect(
    clippy::large_futures,
    reason = "Keep the conformance helper's original await and allocation shape."
)]
async fn embedded_changed_watch_authority_leaves_source_unchanged() {
    changed_original_watch_or_attempt_cannot_accept(true, true).await;
}
#[tokio::test]
#[expect(
    clippy::large_futures,
    reason = "Keep the conformance helper's original await and allocation shape."
)]
async fn surreal_changed_watch_authority_leaves_source_unchanged() {
    changed_original_watch_or_attempt_cannot_accept(false, true).await;
}

#[expect(
    clippy::large_futures,
    reason = "Keep the conformance helper's original await and allocation shape."
)]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture, repository, and mutation owners alive through the original conformance phase."
)]
async fn competing_watch_acceptance_reports_one_actual_insert(embedded: bool) {
    let f = fixture(embedded).await;
    let repo = SourcePublicationRepository::new(f.client.clone());
    let (first, second) = tokio::join!(
        repo.accept_with_watch_producer_outcome(f.request.clone(), &f.producer, None, None),
        repo.accept_with_watch_producer_outcome(f.request.clone(), &f.producer, None, None)
    );
    let first = first.unwrap();
    let second = second.unwrap();
    assert_eq!(
        usize::from(matches!(&first, WatchSourceAcceptance::Accepted(_)))
            + usize::from(matches!(&second, WatchSourceAcceptance::Accepted(_))),
        1
    );
    assert_eq!(
        usize::from(matches!(&first, WatchSourceAcceptance::ReceiptOnly(_)))
            + usize::from(matches!(&second, WatchSourceAcceptance::ReceiptOnly(_))),
        1
    );
    assert_eq!(first.into_receipt(), second.into_receipt());
    let again = repo
        .accept_with_watch_producer_outcome(f.request.clone(), &f.producer, None, None)
        .await
        .unwrap();
    assert!(matches!(again, WatchSourceAcceptance::ReceiptOnly(_)));
}
#[tokio::test]
#[expect(
    clippy::large_futures,
    reason = "Keep the conformance helper's original await and allocation shape."
)]
async fn embedded_competing_watch_acceptance_classifies_actual_transaction() {
    competing_watch_acceptance_reports_one_actual_insert(true).await;
}
#[tokio::test]
#[expect(
    clippy::large_futures,
    reason = "Keep the conformance helper's original await and allocation shape."
)]
async fn surreal_competing_watch_acceptance_classifies_actual_transaction() {
    competing_watch_acceptance_reports_one_actual_insert(false).await;
}

#[expect(
    clippy::large_futures,
    reason = "Keep the conformance helper's original await and allocation shape."
)]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture, repository, and mutation owners alive through the original conformance phase."
)]
async fn queued_watch_intent_cannot_adopt_a_new_requested_uri(embedded: bool) {
    let f = fixture(embedded).await;
    let mut changed = f.producer.watch.clone();
    changed.requested_uri = "https://example.invalid/changed-authority".into();
    let changed = crate::SourceWatchRepository::new(f.client.clone())
        .update("watch-space", &changed.base.id.clone(), changed)
        .await
        .unwrap();
    let rejected = PreparedSourceWatchProducer::capture(
        &f.client,
        &changed,
        "watch-service",
        &f.producer.original_attempt,
        &f.producer.run.base.id,
    )
    .await;
    assert!(
        matches!(rejected,Err(crate::StorageError::Conflict(code)) if code=="watch_producer_queued_intent_changed")
    );
    // Observation-only progress keeps the requested intent hash stable, so
    // changing cadence observations does not invent a new accounting root.
    let mut observed = f.producer.watch.clone();
    observed.next_check_at += chrono::Duration::minutes(1);
    observed.etag = Some("updated-observation".into());
    observed.consecutive_failures = 2;
    assert_eq!(
        watch_intent_fingerprint(&observed, "watch-service").unwrap(),
        watch_intent_fingerprint(&f.producer.watch, "watch-service").unwrap()
    );
    assert_ne!(
        watch_intent_fingerprint(&changed, "watch-service").unwrap(),
        watch_intent_fingerprint(&f.producer.watch, "watch-service").unwrap()
    );
}
#[tokio::test]
#[expect(
    clippy::large_futures,
    reason = "Keep the conformance helper's original await and allocation shape."
)]
async fn embedded_watch_claim_cannot_adopt_changed_requested_intent() {
    queued_watch_intent_cannot_adopt_a_new_requested_uri(true).await;
}
#[tokio::test]
#[expect(
    clippy::large_futures,
    reason = "Keep the conformance helper's original await and allocation shape."
)]
async fn surreal_watch_claim_cannot_adopt_changed_requested_intent() {
    queued_watch_intent_cannot_adopt_a_new_requested_uri(false).await;
}

async fn register_local_delegation(
    client: &StorageClient,
    watch: &SourceWatch,
) -> crate::standing_watch_repository::StandingWatchIdentity {
    let principal = elephant_policy::Principal {
        space_id: watch.base.space_id.clone(),
        scopes: ["read:source", "write:source", "read:doc", "write:doc"]
            .into_iter()
            .map(str::to_owned)
            .collect(),
        clearances: vec![elephant_policy::Clearance {
            policy_id: "default".into(),
            max_level: 0,
            labels: std::collections::HashSet::new(),
            handling_allow: std::collections::HashSet::new(),
        }],
        subject_allowlist: None,
        purpose: std::collections::HashSet::new(),
    };
    let admission =
        elephant_auth::standing_watch::StandingWatchAdmission::trusted_local(&principal).unwrap();
    let registration =
        crate::standing_watch_repository::StandingWatchRepository::new(client.clone())
            .prepare_registration(watch, &admission)
            .await
            .unwrap();
    // This fixture explicitly registers real local authority. The product
    // registration path additionally commits its complete Source/Doc readset.
    if let Some(store) = client.embedded_store() {
        store
            .mutate(|tx| registration.persist_tx(tx))
            .await
            .unwrap();
    } else {
        let mut sql = "BEGIN TRANSACTION;\n".to_owned();
        let mut bindings = std::collections::BTreeMap::new();
        registration
            .append_persist_sql(&mut sql, &mut bindings)
            .unwrap();
        sql.push_str("COMMIT TRANSACTION;");
        client.query_bind_checked(&sql, bindings).await.unwrap();
    }
    registration.identity().clone()
}

#[expect(
    clippy::large_futures,
    reason = "Keep the conformance helper's original await and allocation shape."
)]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture, repository, and mutation owners alive through the original conformance phase."
)]
async fn replaced_delegation_fences_source_acceptance(embedded: bool) {
    let fixture = fixture(embedded).await;
    let repo = SourcePublicationRepository::new(fixture.client.clone());
    let before = repo
        .head("watch-space", &fixture.request.source.base.id)
        .await
        .unwrap();
    let original_watch = fixture.producer.original_watch().clone();
    let replacement = register_local_delegation(&fixture.client, &original_watch).await;
    assert_ne!(
        serde_json::to_value(replacement).unwrap(),
        fixture.producer.original_work().payload["standing_watch_delegation"]
    );
    assert!(repo
        .accept_with_watch_producer(fixture.request.clone(), &fixture.producer, None, None)
        .await
        .is_err());
    assert_eq!(
        repo.head("watch-space", &fixture.request.source.base.id)
            .await
            .unwrap(),
        before
    );
    assert!(repo
        .revision("watch-space", &fixture.request.source.base.id, "watch-r2")
        .await
        .unwrap()
        .is_none());
    assert!(repo
        .operation(
            "watch-space",
            &crate::source_publication::operation_key(
                "watch-space",
                &fixture.request.idempotency_key
            )
        )
        .await
        .unwrap()
        .is_none());
}

#[expect(
    clippy::large_futures,
    reason = "Keep the conformance helper's original await and allocation shape."
)]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture, repository, and mutation owners alive through the original conformance phase."
)]
async fn missing_original_delegation_refuses_capture(embedded: bool) {
    let fixture = fixture(embedded).await;
    // Admit a real original queue item that never carried a delegation. A
    // currently registered Watch must not supply that missing original proof.
    let mut work = fixture.producer.original_work().clone();
    work.base.id = RecordId::new_v4(TableName::WorkItem);
    work.status = WorkItemStatus::Runnable;
    work.attempts = 0;
    work.leased_by = None;
    work.lease_expires_at = None;
    work.idempotency_key = "watch-original-without-delegation".into();
    work.payload
        .as_object_mut()
        .unwrap()
        .remove("standing_watch_delegation");
    let queue = crate::WorkItemRepository::new(fixture.client.clone());
    let queued = queue.create(work).await.unwrap();
    let work = queue
        .claim_with_lease(
            "watch-space",
            &queued.base.id,
            "watch-worker",
            chrono::Utc::now() + chrono::Duration::minutes(5),
        )
        .await
        .unwrap()
        .unwrap();
    let attempt = crate::work_attempt_repository::fence_from_claim(&work).unwrap();
    let mut run = fixture.producer.original_run().clone();
    run.base.id = RecordId::new_v4(TableName::WorkRun);
    run.work_item = Link::new(work.base.id.clone());
    run.attempt = work.attempts;
    run.started_at = chrono::Utc::now();
    run.metrics.custom.insert(
        "attempt_fence".into(),
        serde_json::to_value(&attempt).unwrap(),
    );
    let run = crate::WorkRunRepository::new(fixture.client.clone())
        .create(run)
        .await
        .unwrap();
    assert!(
        matches!(PreparedSourceWatchProducer::capture(
            &fixture.client,
            fixture.producer.original_watch(),
            "watch-service",
            &attempt,
            &run.base.id,
        )
        .await, Err(crate::StorageError::Conflict(ref reason))
            if reason == "standing_watch_authority_changed_or_missing"),
        "current registration cannot be adopted by an unbound original queue item"
    );
    assert_eq!(
        queue.get("watch-space", &work.base.id).await.unwrap(),
        Some(work)
    );
    assert_eq!(
        crate::WorkRunRepository::new(fixture.client.clone())
            .get("watch-space", &run.base.id)
            .await
            .unwrap(),
        Some(run)
    );
    assert!(SourcePublicationRepository::new(fixture.client)
        .revision("watch-space", &fixture.request.source.base.id, "watch-r2")
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
#[expect(
    clippy::large_futures,
    reason = "Keep the conformance helper's original await and allocation shape."
)]
async fn embedded_replaced_watch_delegation_prevents_source_acceptance() {
    replaced_delegation_fences_source_acceptance(true).await;
}
#[tokio::test]
#[expect(
    clippy::large_futures,
    reason = "Keep the conformance helper's original await and allocation shape."
)]
async fn surreal_replaced_watch_delegation_prevents_source_acceptance() {
    replaced_delegation_fences_source_acceptance(false).await;
}
#[tokio::test]
#[expect(
    clippy::large_futures,
    reason = "Keep the conformance helper's original await and allocation shape."
)]
async fn embedded_missing_original_watch_delegation_is_not_adopted() {
    missing_original_delegation_refuses_capture(true).await;
}
#[tokio::test]
#[expect(
    clippy::large_futures,
    reason = "Keep the conformance helper's original await and allocation shape."
)]
async fn surreal_missing_original_watch_delegation_is_not_adopted() {
    missing_original_delegation_refuses_capture(false).await;
}

#[path = "source_watch_canonical_tests.rs"]
mod canonical_standing;
