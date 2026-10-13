//! Actual original queue claims guard intake staging and watch observations.
#![allow(clippy::unwrap_used)]
use super::*;
use crate::source_watch_producer::{watch_intent_fingerprint, PreparedSourceWatchProducer};
use crate::{Repository, SourceWatchRepository, WorkItemRepository, WorkRunRepository};
use chrono::Utc;
use elephant_types::records::{
    SourceType, SourceWatch, SourceWatchCheck, SourceWatchOutcome, SourceWatchStatus, WorkItem,
    WorkItemStatus, WorkRun,
};

const SPACE: &str = "household";
const ACTOR: &str = "watch-service";

struct Fixture {
    storage: StorageClient,
    watch: SourceWatch,
    work: WorkItem,
    producer: PreparedSourceWatchProducer,
    prepared: PreparedSourceIntake,
}

#[expect(
    clippy::too_many_lines,
    reason = "Keep this ordered conformance scenario and its failure assertions in one function."
)]
async fn fixture(embedded: bool) -> Fixture {
    let storage = Box::pin(super::tests::backend(embedded)).await;
    if !embedded {
        for sql in [
            include_str!("../../migrations/sql/022_source_watches.surql"),
            include_str!("../../migrations/sql/032_work_recovery.surql"),
            include_str!("../../migrations/sql/039_private_reference_fence.surql"),
            include_str!("../../migrations/sql/041_work_producer_origin.surql"),
            include_str!("../../migrations/sql/042_watch_delegation.surql"),
            include_str!("../../migrations/sql/043_native_read_fence.surql"),
        ] {
            storage.query_checked(sql).await.unwrap();
        }
    }
    let mut input = super::tests::source(SPACE, "Original watched document");
    input.type_ = SourceType::Web;
    input.uri = Some("https://example.invalid/watched".into());
    let initial = Box::pin(PreparedSourceIntake::prepare(
        &storage,
        input,
        IntakeControl::default(),
    ))
    .await
    .unwrap();
    let accepted = Box::pin(initial.accept(&storage)).await.unwrap();
    // Acceptance returns the immutable input, while the live Source carries
    // the commit timestamp. Authorize the actual original live row.
    let original_live_source =
        Box::pin(SourceRepository::new(storage.clone()).get(SPACE, &accepted.source.base.id))
            .await
            .unwrap()
            .unwrap();
    let mut watch_base = accepted.source.base.clone();
    watch_base.id = RecordId::new_v4(TableName::SourceWatch);
    let watch = SourceWatch {
        base: watch_base,
        source: Link::new(accepted.source.base.id.clone()),
        requested_uri: accepted.source.uri.clone().unwrap(),
        status: SourceWatchStatus::Enabled,
        interval_seconds: 60,
        next_check_at: Utc::now(),
        last_checked_at: None,
        last_changed_at: None,
        etag: None,
        last_modified: None,
        observed_hash: Some(accepted.source.hash.clone()),
        consecutive_failures: 0,
        last_error: None,
        latest_check: None,
    };
    let watch = Box::pin(SourceWatchRepository::new(storage.clone()).create(watch))
        .await
        .unwrap();
    let principal = local_principal(&watch);
    let admission =
        elephant_auth::standing_watch::StandingWatchAdmission::trusted_local(&principal).unwrap();
    let registration = crate::source_watch_registration::PreparedSourceWatchRegistration::prepare(
        &storage,
        &original_live_source,
        Some(&watch),
        watch.clone(),
    )
    .await
    .unwrap();
    let standing = crate::standing_watch_repository::StandingWatchRepository::new(storage.clone())
        .prepare_registration(&watch, &admission)
        .await
        .unwrap();
    let (_, delegation) = registration.register(&storage, &standing).await.unwrap();
    let intent = watch_intent_fingerprint(&watch, ACTOR).unwrap();
    let mut base = watch.base.clone();
    base.id = RecordId::new_v4(TableName::WorkItem);
    base.created_by = ACTOR.into();
    let work = WorkItem {
        base,
        kind: "refresh_source_watch".into(),
        payload: serde_json::json!({"watch_id":watch.base.id,"watch_intent_fingerprint":intent,
            "standing_watch_delegation":delegation}),
        status: WorkItemStatus::Runnable,
        priority: 0,
        idempotency_key: "original-watch-intent".into(),
        attempts: 0,
        lease_expires_at: None,
        leased_by: None,
        run_after: None,
        last_error: None,
    };
    let queue = WorkItemRepository::new(storage.clone());
    let work = Box::pin(queue.create(work)).await.unwrap();
    let work = Box::pin(queue.claim_with_lease(
        SPACE,
        &work.base.id,
        "watch-worker",
        Utc::now() + chrono::Duration::minutes(5),
    ))
    .await
    .unwrap()
    .unwrap();
    let attempt = crate::work_attempt_repository::fence_from_claim(&work).unwrap();
    let mut base = work.base.clone();
    base.id = RecordId::new_v4(TableName::WorkRun);
    let mut run = WorkRun {
        base,
        work_item: Link::new(work.base.id.clone()),
        attempt: work.attempts,
        started_at: Utc::now(),
        completed_at: None,
        success: false,
        error: None,
        metrics: elephant_types::records::WorkRunMetrics::default(),
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
    let run = Box::pin(WorkRunRepository::new(storage.clone()).create(run))
        .await
        .unwrap();
    let producer = Box::pin(PreparedSourceWatchProducer::capture(
        &storage,
        &watch,
        ACTOR,
        &attempt,
        &run.base.id,
    ))
    .await
    .unwrap();
    let mut input = accepted.source.clone();
    let mut template = input.manual_doc_template().unwrap().unwrap();
    template.text = "Replacement watched document".into();
    input.hash = "replacement-watch-hash".into();
    input.set_manual_doc_template(&template).unwrap();
    let prepared = Box::pin(PreparedSourceIntake::prepare(
        &storage,
        input,
        IntakeControl {
            revision_id: Some("watch-r2".into()),
            idempotency_key: Some("original-watch-r2".into()),
            expected_generation: Some(accepted.publication.generation),
        },
    ))
    .await
    .unwrap()
    .with_authorized_snapshot(SourceAuthorizationSnapshot::from(&original_live_source));
    Fixture {
        storage,
        watch,
        work,
        producer,
        prepared,
    }
}

fn local_principal(watch: &SourceWatch) -> elephant_policy::Principal {
    elephant_policy::Principal {
        space_id: watch.base.space_id.clone(),
        scopes: [
            "read:source",
            "write:source",
            "read:doc",
            "write:doc",
            "read:entity",
            "write:entity",
            "read:relationship",
            "write:relationship",
        ]
        .into_iter()
        .map(str::to_owned)
        .collect(),
        clearances: vec![elephant_policy::Clearance {
            policy_id: watch.base.security.policy_id.clone(),
            max_level: watch.base.security.level,
            labels: watch.base.security.labels.iter().cloned().collect(),
            handling_allow: watch.base.security.handling.iter().cloned().collect(),
        }],
        subject_allowlist: None,
        purpose: std::collections::HashSet::default(),
    }
}

fn check(watch: &SourceWatch) -> SourceWatchCheck {
    let mut base = watch.base.clone();
    base.id = RecordId::new_v4(TableName::SourceWatchCheck);
    SourceWatchCheck {
        base,
        watch: Link::new(watch.base.id.clone()),
        source: watch.source.clone(),
        outcome: SourceWatchOutcome::NotModified,
        started_at: Utc::now(),
        completed_at: Utc::now(),
        requested_uri: watch.requested_uri.clone(),
        final_uri: Some(watch.requested_uri.clone()),
        content_type: None,
        etag: None,
        last_modified: None,
        prior_hash: watch.observed_hash.clone(),
        observed_hash: watch.observed_hash.clone(),
        content_length: None,
        error: None,
    }
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep f, repo, watches in the original fixture scope through assertions and cleanup."
)]
#[expect(
    clippy::too_many_lines,
    reason = "Keep this ordered conformance scenario and its failure assertions in one function."
)]
async fn receipt_retry_and_original_observation(embedded: bool) {
    let f = Box::pin(fixture(embedded)).await;
    let accepted = Box::pin(f.prepared.clone().accept_with_watch_producer(
        &f.storage,
        &f.producer,
        None,
    ))
    .await
    .unwrap();
    assert!(accepted.needs_processing());
    let doc = accepted.document.clone().unwrap();
    assert_eq!(
        Box::pin(crate::DocRepository::new(f.storage.clone()).get(SPACE, &doc.base.id))
            .await
            .unwrap(),
        Some(doc.clone())
    );
    // Delete exactly the staged projection, then replay the SAME prepared
    // original request. A retry cannot restage missing historical evidence.
    Box::pin(crate::DocRepository::new(f.storage.clone()).hard_delete(SPACE, &doc.base.id))
        .await
        .unwrap();
    let repo = SourcePublicationRepository::new(f.storage.clone());
    let head = repo.head(SPACE, &accepted.source.base.id).await.unwrap();
    let source = crate::SourceRepository::new(f.storage.clone())
        .get(SPACE, &accepted.source.base.id)
        .await
        .unwrap();
    let replay = Box::pin(
        f.prepared
            .accept_with_watch_producer(&f.storage, &f.producer, None),
    )
    .await
    .unwrap();
    assert!(!replay.needs_processing());
    assert_eq!(replay.operation, accepted.operation);
    assert_eq!(replay.publication, accepted.publication);
    assert_eq!(replay.source, accepted.source);
    assert!(crate::DocRepository::new(f.storage.clone())
        .get(SPACE, &doc.base.id)
        .await
        .unwrap()
        .is_none());
    assert_eq!(
        repo.head(SPACE, &accepted.source.base.id).await.unwrap(),
        head
    );
    assert_eq!(
        crate::SourceRepository::new(f.storage.clone())
            .get(SPACE, &accepted.source.base.id)
            .await
            .unwrap(),
        source
    );
    assert_eq!(
        WorkItemRepository::new(f.storage.clone())
            .get(SPACE, &f.work.base.id)
            .await
            .unwrap(),
        Some(f.work.clone())
    );
    let watches = SourceWatchRepository::new(f.storage.clone());
    assert_eq!(
        watches.get(SPACE, &f.watch.base.id).await.unwrap(),
        Some(f.watch.clone())
    );
    assert!(watches
        .list_checks(SPACE, &f.watch.base.id, 10)
        .await
        .unwrap()
        .is_empty());
    let receipt = check(&f.watch);
    let (updated, recorded) = Box::pin(watches.record_check_with_watch_producer(
        f.watch.clone(),
        receipt.clone(),
        &f.producer,
    ))
    .await
    .unwrap();
    assert_eq!(recorded, receipt);
    assert_eq!(updated.latest_check, Some(receipt.base.id.clone()));
    assert!(
        Box::pin(watches.record_check_with_watch_producer(
            f.watch.clone(),
            check(&f.watch),
            &f.producer,
        ))
        .await
        .is_err(),
        "old watch snapshot cannot overwrite an observed successor"
    );
    assert_eq!(
        watches
            .list_checks(SPACE, &f.watch.base.id, 10)
            .await
            .unwrap(),
        vec![receipt]
    );
    assert_eq!(
        watches.get(SPACE, &f.watch.base.id).await.unwrap(),
        Some(updated)
    );
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep f, repo, watches in the original fixture scope through assertions and cleanup."
)]
async fn cancel_after_acceptance_prevents_staging_and_check(embedded: bool) {
    let f = Box::pin(fixture(embedded)).await;
    // Real acceptance without adapter staging isolates the acceptance->Doc gap.
    let repo = SourcePublicationRepository::new(f.storage.clone());
    let receipt = Box::pin(repo.accept_with_watch_producer(
        f.prepared.request.clone(),
        &f.producer,
        None,
        None,
    ))
    .await
    .unwrap();
    let accepted = Box::pin(PreparedSourceIntake::load_accepted(
        &f.storage,
        receipt.clone(),
        false,
    ))
    .await
    .unwrap();
    let doc = accepted.document.unwrap();
    let head = repo.head(SPACE, &accepted.source.base.id).await.unwrap();
    let original_attempt = crate::work_attempt_repository::fence_from_claim(&f.work).unwrap();
    let cancelled = Box::pin(
        WorkItemRepository::new(f.storage.clone()).update_running_for_worker(
            SPACE,
            &f.work.base.id,
            &original_attempt.lease_token,
            WorkItemStatus::Cancelled,
            Some("original watch cancelled after source acceptance".into()),
        ),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(cancelled.status, WorkItemStatus::Cancelled);
    let staging = Box::pin(stage_source_revision_mode(
        &f.storage,
        &accepted.source,
        &accepted.publication,
        Some(&f.producer),
    ))
    .await;
    assert!(
        matches!(&staging, Err(StorageError::Conflict(reason)) if reason.contains("stale_attempt")),
        "cancelled original watch attempt must refuse staging: {staging:?}"
    );
    assert!(crate::DocRepository::new(f.storage.clone())
        .get(SPACE, &doc.base.id)
        .await
        .unwrap()
        .is_none());
    let watches = SourceWatchRepository::new(f.storage.clone());
    let observation = Box::pin(watches.record_check_with_watch_producer(
        f.watch.clone(),
        check(&f.watch),
        &f.producer,
    ))
    .await;
    assert!(
        matches!(&observation, Err(StorageError::Conflict(reason)) if reason.contains("stale_attempt")),
        "cancelled original watch attempt must refuse its observation: {observation:?}"
    );
    assert_eq!(
        watches.get(SPACE, &f.watch.base.id).await.unwrap(),
        Some(f.watch.clone())
    );
    assert!(watches
        .list_checks(SPACE, &f.watch.base.id, 10)
        .await
        .unwrap()
        .is_empty());
    assert_eq!(
        WorkItemRepository::new(f.storage.clone())
            .get(SPACE, &f.work.base.id)
            .await
            .unwrap(),
        Some(cancelled)
    );
    assert_eq!(
        repo.head(SPACE, &accepted.source.base.id).await.unwrap(),
        head
    );
    assert_eq!(
        repo.matching_source_receipt(&f.prepared.request)
            .await
            .unwrap(),
        Some(receipt)
    );
}

#[tokio::test]
async fn embedded_watch_intake_retry_is_read_only_and_check_is_original() {
    Box::pin(receipt_retry_and_original_observation(true)).await;
}
#[tokio::test]
async fn surreal_watch_intake_retry_is_read_only_and_check_is_original() {
    Box::pin(receipt_retry_and_original_observation(false)).await;
}
#[tokio::test]
async fn embedded_cancel_after_watch_acceptance_prevents_document_and_check() {
    Box::pin(cancel_after_acceptance_prevents_staging_and_check(true)).await;
}
#[tokio::test]
async fn surreal_cancel_after_watch_acceptance_prevents_document_and_check() {
    Box::pin(cancel_after_acceptance_prevents_staging_and_check(false)).await;
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep f, sources, publications, docs, watches in the original fixture scope through assertions and cleanup."
)]
#[expect(
    clippy::too_many_lines,
    reason = "Keep this ordered conformance scenario and its failure assertions in one function."
)]
async fn current_input_observation_guards_source_and_head(embedded: bool) {
    use crate::source_watch_repository::PreparedSourceWatchObservation;
    for change in ["unchanged", "source", "acceptance"] {
        let f = Box::pin(fixture(embedded)).await;
        let sources = crate::SourceRepository::new(f.storage.clone());
        let publications = SourcePublicationRepository::new(f.storage.clone());
        let source = Box::pin(sources.get(SPACE, f.watch.source.id()))
            .await
            .unwrap()
            .unwrap();
        let head = Box::pin(publications.head(SPACE, &source.base.id))
            .await
            .unwrap()
            .unwrap();
        let observation = Box::pin(PreparedSourceWatchObservation::capture(
            &f.storage, &f.watch, &source, &head,
        ))
        .await
        .unwrap();
        match change {
            "source" => {
                let mut changed = source.clone();
                changed.base.security.level += 1;
                Box::pin(sources.update(SPACE, &source.base.id, changed))
                    .await
                    .unwrap();
            }
            "acceptance" => {
                let accepted = Box::pin(f.prepared.clone().accept_with_watch_producer(
                    &f.storage,
                    &f.producer,
                    None,
                ))
                .await
                .unwrap();
                assert_ne!(accepted.publication.generation, head.generation);
                assert!(
                    Box::pin(PreparedSourceWatchObservation::capture(
                        &f.storage, &f.watch, &source, &head,
                    ))
                    .await
                    .is_err(),
                    "capture cannot substitute a new raw head"
                );
            }
            _ => {}
        }
        let current_source = Box::pin(sources.get(SPACE, &source.base.id)).await.unwrap();
        let current_head = Box::pin(publications.head(SPACE, &source.base.id))
            .await
            .unwrap();
        let current_receipt =
            Box::pin(publications.operation(SPACE, &current_head.as_ref().unwrap().operation_id))
                .await
                .unwrap();
        let docs = crate::DocRepository::new(f.storage.clone());
        let mut current_docs = Box::pin(docs.find_by_source(SPACE, &source.base.id, 10))
            .await
            .unwrap();
        current_docs.sort_by_key(|doc| doc.base.id.to_string());
        let mut receipt = check(&f.watch);
        receipt.outcome = SourceWatchOutcome::HashUnchanged;
        let watches = SourceWatchRepository::new(f.storage.clone());
        let result = Box::pin(watches.record_check_with_input_observation(
            f.watch.clone(),
            receipt.clone(),
            Some(&f.producer),
            &observation,
        ))
        .await;
        if change == "unchanged" {
            let (watch, recorded) = result.unwrap();
            assert_eq!(recorded, receipt);
            assert_eq!(watch.latest_check, Some(receipt.base.id.clone()));
            assert_eq!(
                Box::pin(watches.list_checks(SPACE, &f.watch.base.id, 10))
                    .await
                    .unwrap(),
                vec![receipt]
            );
        } else {
            assert!(
                matches!(result, Err(StorageError::Conflict(_))),
                "stale original {change} must refuse: {result:?}"
            );
            assert_eq!(
                Box::pin(watches.get(SPACE, &f.watch.base.id))
                    .await
                    .unwrap(),
                Some(f.watch.clone())
            );
            assert!(Box::pin(watches.list_checks(SPACE, &f.watch.base.id, 10))
                .await
                .unwrap()
                .is_empty());
        }
        assert_eq!(
            Box::pin(sources.get(SPACE, &source.base.id)).await.unwrap(),
            current_source
        );
        assert_eq!(
            Box::pin(publications.head(SPACE, &source.base.id))
                .await
                .unwrap(),
            current_head
        );
        assert_eq!(
            Box::pin(publications.operation(SPACE, &current_head.unwrap().operation_id,))
                .await
                .unwrap(),
            current_receipt
        );
        let mut after_docs = Box::pin(docs.find_by_source(SPACE, &source.base.id, 10))
            .await
            .unwrap();
        after_docs.sort_by_key(|doc| doc.base.id.to_string());
        assert_eq!(after_docs, current_docs);
        assert_eq!(
            Box::pin(WorkItemRepository::new(f.storage.clone()).get(SPACE, &f.work.base.id))
                .await
                .unwrap(),
            Some(f.work)
        );
    }
}

#[tokio::test]
async fn embedded_watch_current_input_observation_guards_original_source_and_head() {
    Box::pin(current_input_observation_guards_source_and_head(true)).await;
}

#[tokio::test]
async fn surreal_watch_current_input_observation_guards_original_source_and_head() {
    Box::pin(current_input_observation_guards_source_and_head(false)).await;
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep f, sources, standing, configs, publications in the original fixture scope through assertions and cleanup."
)]
#[expect(
    clippy::too_many_lines,
    reason = "Keep this ordered conformance scenario and its failure assertions in one function."
)]
async fn standing_registration_guards_all_original_records(embedded: bool) {
    use crate::source_watch_registration::PreparedSourceWatchRegistration;
    use crate::standing_watch_repository::StandingWatchRepository;
    for change in [
        "unchanged",
        "source",
        "document",
        "watch",
        "head",
        "configuration",
    ] {
        let f = Box::pin(fixture(embedded)).await;
        let sources = crate::SourceRepository::new(f.storage.clone());
        let source = sources
            .get(SPACE, f.watch.source.id())
            .await
            .unwrap()
            .unwrap();
        let mut proposed = f.watch.clone();
        proposed.interval_seconds = 120;
        let records = Box::pin(PreparedSourceWatchRegistration::prepare(
            &f.storage,
            &source,
            Some(&f.watch),
            proposed.clone(),
        ))
        .await
        .unwrap();
        let admission = elephant_auth::standing_watch::StandingWatchAdmission::trusted_local(
            &local_principal(&f.watch),
        )
        .unwrap();
        records.authorize(admission.principal()).unwrap();
        let standing = StandingWatchRepository::new(f.storage.clone());
        let old_identity = standing
            .capture_for_scheduling(&f.watch)
            .await
            .unwrap()
            .identity()
            .clone();
        let registration = standing
            .prepare_registration(&proposed, &admission)
            .await
            .unwrap();
        assert_ne!(registration.identity(), &old_identity);
        match change {
            "source" => {
                let mut changed = source.clone();
                changed.base.security.level += 1;
                sources
                    .update(SPACE, &source.base.id, changed)
                    .await
                    .unwrap();
            }
            "document" => {
                let mut changed = records.documents().first().unwrap().clone();
                changed.base.security.level += 1;
                crate::DocRepository::new(f.storage.clone())
                    .update(SPACE, &changed.base.id.clone(), changed)
                    .await
                    .unwrap();
            }
            "watch" => {
                let mut changed = f.watch.clone();
                changed.last_error = Some("concurrent original observation".into());
                SourceWatchRepository::new(f.storage.clone())
                    .update(SPACE, &changed.base.id.clone(), changed)
                    .await
                    .unwrap();
            }
            "head" => {
                Box::pin(
                    f.prepared
                        .accept_with_watch_producer(&f.storage, &f.producer, None),
                )
                .await
                .unwrap();
            }
            "configuration" => {
                let configs = crate::SpaceConfigRepository::new(f.storage.clone());
                let mut changed = configs.get_by_space_id(SPACE).await.unwrap().unwrap();
                changed.purpose = Some("changed extraction configuration".into());
                configs
                    .update(SPACE, &changed.base.id.clone(), changed)
                    .await
                    .unwrap();
            }
            _ => {}
        }
        let before_source = sources.get(SPACE, &source.base.id).await.unwrap();
        let before_watch = SourceWatchRepository::new(f.storage.clone())
            .get(SPACE, &f.watch.base.id)
            .await
            .unwrap();
        let publications = SourcePublicationRepository::new(f.storage.clone());
        let before_head = publications.head(SPACE, &source.base.id).await.unwrap();
        let result = Box::pin(records.register(&f.storage, &registration)).await;
        if change == "unchanged" {
            let (actual, identity) = result.unwrap();
            assert_eq!(actual, proposed);
            assert_eq!(&identity, registration.identity());
            assert_eq!(
                standing
                    .capture_for_scheduling(&actual)
                    .await
                    .unwrap()
                    .identity(),
                &identity
            );
            assert!(SourceWatchRepository::new(f.storage.clone()).record_check_with_watch_producer(
                f.watch.clone(), check(&f.watch), &f.producer,
            ).await.is_err(), "original work cannot adopt the explicit replacement delegation");
        } else {
            assert!(result.is_err(), "stale {change} must prevent registration");
            let actual = SourceWatchRepository::new(f.storage.clone())
                .get(SPACE, &f.watch.base.id)
                .await
                .unwrap();
            assert_eq!(actual, before_watch);
            // Every mutation above keeps the Watch's semantic registration
            // intent unchanged. The old delegation remains intact on failure.
            assert_eq!(
                standing
                    .capture_for_scheduling(actual.as_ref().unwrap())
                    .await
                    .unwrap()
                    .identity(),
                &old_identity
            );
        }
        assert_eq!(
            sources.get(SPACE, &source.base.id).await.unwrap(),
            before_source
        );
        assert_eq!(
            publications.head(SPACE, &source.base.id).await.unwrap(),
            before_head
        );
        assert!(SourceWatchRepository::new(f.storage.clone())
            .list_checks(SPACE, &f.watch.base.id, 10)
            .await
            .unwrap()
            .is_empty());
    }
}

#[tokio::test]
async fn embedded_standing_registration_fences_source_doc_watch_head_and_configuration() {
    Box::pin(standing_registration_guards_all_original_records(true)).await;
}

#[tokio::test]
async fn surreal_standing_registration_fences_source_doc_watch_head_and_configuration() {
    Box::pin(standing_registration_guards_all_original_records(false)).await;
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep f, docs, publications in the original fixture scope through assertions and cleanup."
)]
async fn recovery_document_staging_retains_original_watch_delegation(embedded: bool) {
    for change in ["unchanged", "delegation", "source"] {
        let f = Box::pin(fixture(embedded)).await;
        let accepted = Box::pin(f.prepared.accept_with_watch_producer(
            &f.storage,
            &f.producer,
            None,
        ))
        .await
        .unwrap();
        let doc = accepted.document.as_ref().unwrap();
        let docs = crate::DocRepository::new(f.storage.clone());
        // Isolate accepted-input recovery with no staged projection. This is
        // fault injection only, not a physical-erasure qualification oracle.
        docs.hard_delete(SPACE, &doc.base.id).await.unwrap();
        let mut current_source = SourceRepository::new(f.storage.clone())
            .get(SPACE, &accepted.source.base.id)
            .await
            .unwrap()
            .unwrap();
        if change == "source" {
            current_source.base.security.level += 1;
            SourceRepository::new(f.storage.clone())
                .update(
                    SPACE,
                    &current_source.base.id.clone(),
                    current_source.clone(),
                )
                .await
                .unwrap();
        }
        if change == "delegation" {
            let mut proposed = f.watch.clone();
            proposed.interval_seconds += 60;
            let admission = elephant_auth::standing_watch::StandingWatchAdmission::trusted_local(
                &local_principal(&f.watch),
            )
            .unwrap();
            let records = Box::pin(
                crate::source_watch_registration::PreparedSourceWatchRegistration::prepare(
                    &f.storage,
                    &current_source,
                    Some(&f.watch),
                    proposed.clone(),
                ),
            )
            .await
            .unwrap();
            let registration =
                crate::standing_watch_repository::StandingWatchRepository::new(f.storage.clone())
                    .prepare_registration(&proposed, &admission)
                    .await
                    .unwrap();
            Box::pin(records.register(&f.storage, &registration))
                .await
                .unwrap();
        }
        let publications = SourcePublicationRepository::new(f.storage.clone());
        let head = publications
            .head(SPACE, &accepted.source.base.id)
            .await
            .unwrap();
        let receipt = publications
            .operation(SPACE, &accepted.operation.operation_id)
            .await
            .unwrap();
        // Actual recovery entry has no original live Watch attempt parameter.
        // It must follow the saved operation origin, not infer a direct caller.
        let result = Box::pin(stage_source_revision(
            &f.storage,
            &accepted.source,
            &accepted.publication,
        ))
        .await;
        if change == "unchanged" {
            result.unwrap();
            assert_eq!(
                docs.get(SPACE, &doc.base.id).await.unwrap().as_ref(),
                Some(doc)
            );
        } else {
            assert!(result.is_err());
            assert!(docs.get(SPACE, &doc.base.id).await.unwrap().is_none());
        }
        assert_eq!(
            publications
                .head(SPACE, &accepted.source.base.id)
                .await
                .unwrap(),
            head
        );
        assert_eq!(
            publications
                .operation(SPACE, &accepted.operation.operation_id)
                .await
                .unwrap(),
            receipt
        );
    }
}

#[tokio::test]
async fn embedded_recovery_doc_staging_uses_original_watch_delegation() {
    Box::pin(recovery_document_staging_retains_original_watch_delegation(
        true,
    ))
    .await;
}

#[tokio::test]
async fn surreal_recovery_doc_staging_uses_original_watch_delegation() {
    Box::pin(recovery_document_staging_retains_original_watch_delegation(
        false,
    ))
    .await;
}

// Exercise the production native read-set builder with simultaneous transactions
// without StorageClient's ordinary automatic conflict retry. This is a guarded
// observation race oracle, not a source-publication completion fixture.
#[expect(
    clippy::expect_used,
    reason = "Missing the original native writer revision must fail this test immediately."
)]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep f, publications in the original fixture scope through assertions and cleanup."
)]
#[expect(
    clippy::too_many_lines,
    reason = "Keep this ordered conformance scenario and its failure assertions in one function."
)]
async fn native_observation_conflicts_with_original_input_writer(head_only: bool) {
    use crate::source_publication::{control_thing, head_key, HEAD_TABLE};
    use crate::source_watch_repository::PreparedSourceWatchObservation;
    let f = Box::pin(fixture(false)).await;
    let publications = SourcePublicationRepository::new(f.storage.clone());
    let source = crate::SourceRepository::new(f.storage.clone())
        .get(SPACE, f.watch.source.id())
        .await
        .unwrap()
        .unwrap();
    let head = publications
        .head(SPACE, &source.base.id)
        .await
        .unwrap()
        .unwrap();
    let observation = PreparedSourceWatchObservation::capture(&f.storage, &f.watch, &source, &head)
        .await
        .unwrap();
    let mut bindings = std::collections::BTreeMap::new();
    let mut guard = String::new();
    observation
        .append_guard_sql(&mut guard, &mut bindings)
        .unwrap();
    let control_bindings = crate::bindings! {
        "head" => control_thing(HEAD_TABLE, &head_key(SPACE, &source.base.id)),
        "writer_guard" => surrealdb::sql::Thing::from(("source_erasure_guard", SPACE)),
    };
    let mut before_control = f.storage.query_bind_checked(
        "SELECT VALUE canonical_read_nonce FROM ONLY $head; SELECT VALUE revision FROM ONLY $writer_guard;",
        control_bindings.clone(),
    ).await.unwrap();
    let before_nonce: Option<String> = before_control.take(0).unwrap();
    let before_revision: Option<i64> = before_control.take(1).unwrap();
    let before_revision = before_revision.expect("original native Source writer revision");
    // A successful standalone guard preserves the complete original input.
    f.storage
        .query_bind_checked(
            &format!("BEGIN TRANSACTION; {guard} COMMIT TRANSACTION;"),
            bindings.clone(),
        )
        .await
        .unwrap();
    assert_eq!(
        publications.head(SPACE, &source.base.id).await.unwrap(),
        Some(head.clone())
    );
    assert_eq!(
        crate::SourceRepository::new(f.storage.clone())
            .get(SPACE, &source.base.id)
            .await
            .unwrap(),
        Some(source.clone())
    );
    let mut after_control = f.storage.query_bind_checked(
        "SELECT VALUE canonical_read_nonce FROM ONLY $head; SELECT VALUE revision FROM ONLY $writer_guard;",
        control_bindings,
    ).await.unwrap();
    let after_nonce: Option<String> = after_control.take(0).unwrap();
    let after_revision: Option<i64> = after_control.take(1).unwrap();
    let after_revision = after_revision.expect("committed native Source writer revision");
    assert!(after_nonce.is_some());
    assert_ne!(
        after_nonce, before_nonce,
        "present head must receive a REAL changed private nonce"
    );
    assert!(
        after_revision > before_revision,
        "Source writer fence must perform a REAL shared revision change"
    );
    let observation_sql = format!(
        "BEGIN TRANSACTION; {guard} SLEEP 200ms; UPDATE $watch_input_watch SET last_error='observation-guard-committed'; COMMIT TRANSACTION;"
    );
    let mut changed_head = head.clone();
    changed_head.updated_at += chrono::Duration::seconds(1);
    let writer_bindings = crate::bindings! {
        "head" => control_thing(HEAD_TABLE, &head_key(SPACE, &source.base.id)),
        "head_before" => serde_json::to_string(&head).unwrap(),
        "head_after" => serde_json::to_string(&changed_head).unwrap(),
        "source" => crate::surreal::thing_from_record_id(&source.base.id),
        "source_hash" => source.hash.clone(),
    };
    let writer_sql = if head_only {
        "BEGIN TRANSACTION; IF (SELECT VALUE payload FROM ONLY $head) != $head_before { THROW 'test_original_head_changed'; }; SLEEP 200ms; UPDATE $head SET payload=$head_after; COMMIT TRANSACTION;"
    } else {
        "BEGIN TRANSACTION; IF (SELECT VALUE hash FROM ONLY $source) != $source_hash { THROW 'test_original_source_changed'; }; SLEEP 200ms; UPDATE $source SET hash='independent-source-write'; COMMIT TRANSACTION;"
    };
    let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(2));
    let observe = async {
        barrier.wait().await;
        f.storage
            .query_bind(&observation_sql, bindings)
            .await
            .unwrap()
            .check()
    };
    let writer = async {
        barrier.wait().await;
        f.storage
            .query_bind(writer_sql, writer_bindings)
            .await
            .unwrap()
            .check()
    };
    let (observation_result, writer_result) = tokio::join!(observe, writer);
    assert_ne!(observation_result.is_ok(), writer_result.is_ok(),
        "exactly one simultaneous transaction can commit: observation={observation_result:?}, writer={writer_result:?}");
    let current_watch = SourceWatchRepository::new(f.storage.clone())
        .get(SPACE, &f.watch.base.id)
        .await
        .unwrap()
        .unwrap();
    if observation_result.is_ok() {
        assert_eq!(
            current_watch.last_error.as_deref(),
            Some("observation-guard-committed")
        );
        assert_eq!(
            publications.head(SPACE, &source.base.id).await.unwrap(),
            Some(head)
        );
        assert_eq!(
            crate::SourceRepository::new(f.storage.clone())
                .get(SPACE, &source.base.id)
                .await
                .unwrap(),
            Some(source)
        );
    } else {
        assert_eq!(current_watch, f.watch);
        if head_only {
            assert_eq!(
                publications.head(SPACE, &source.base.id).await.unwrap(),
                Some(changed_head)
            );
            assert_eq!(
                crate::SourceRepository::new(f.storage.clone())
                    .get(SPACE, &source.base.id)
                    .await
                    .unwrap(),
                Some(source)
            );
        } else {
            assert_eq!(
                publications.head(SPACE, &source.base.id).await.unwrap(),
                Some(head)
            );
            assert_eq!(
                crate::SourceRepository::new(f.storage.clone())
                    .get(SPACE, &source.base.id)
                    .await
                    .unwrap()
                    .unwrap()
                    .hash,
                "independent-source-write"
            );
        }
    }
}

#[tokio::test]
async fn native_watch_observation_conflicts_with_concurrent_head_only_writer() {
    Box::pin(native_observation_conflicts_with_original_input_writer(
        true,
    ))
    .await;
}

#[tokio::test]
async fn native_watch_observation_conflicts_with_concurrent_source_only_writer() {
    Box::pin(native_observation_conflicts_with_original_input_writer(
        false,
    ))
    .await;
}
