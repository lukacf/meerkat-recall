//! Original acceptance-to-enqueue race; source publication is already durable.
use super::*;
use crate::source_publication::{AcceptSourceRevision, SourcePublicationRepository};
use crate::source_watch_producer::PreparedSourceWatchProducer;
use elephant_types::publication::{PublicationFence, SourcePrecondition};
use elephant_types::records::{Source, SourceType, SourceWatch, SourceWatchStatus, SpaceConfig};

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep storage, publications, queue, origins in the original fixture scope through assertions and cleanup."
)]
async fn contract(embedded: bool, cancel: bool) {
    let storage = if embedded {
        Box::pin(StorageClient::new_embedded())
            .await
            .expect("embedded")
    } else {
        Box::pin(StorageClient::new_memory()).await.expect("native")
    };
    if !embedded {
        for sql in [
            include_str!("../../migrations/sql/031_source_publication.surql"),
            include_str!("../../migrations/sql/032_work_recovery.surql"),
            include_str!("../../migrations/sql/039_private_reference_fence.surql"),
            include_str!("../../migrations/sql/041_work_producer_origin.surql"),
            include_str!("../../migrations/sql/042_watch_delegation.surql"),
            include_str!("../../migrations/sql/043_native_read_fence.surql"),
        ] {
            storage
                .query_checked(sql)
                .await
                .expect("explicit fixture schema");
        }
    }
    let space = "watch-queue-race";
    let base = |table| {
        BaseFields::new(
            RecordId::new_v4(table),
            space.into(),
            "watch-service".into(),
        )
    };
    crate::SpaceConfigRepository::new(storage.clone())
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
        .expect("named space");
    let publications = SourcePublicationRepository::new(storage.clone());
    let mut request = AcceptSourceRevision {
        source: Source::new(
            base(TableName::Source),
            SourceType::Web,
            "original-hash".into(),
        ),
        revision_id: "original-r1".into(),
        expected: SourcePrecondition::Absent,
        idempotency_key: "original-accept".into(),
        request_fingerprint: "original-input".into(),
        reactivate: false,
        recreate_after_erasure: false,
        authorized: None,
        authorized_document: None,
    };
    let first = Box::pin(publications.accept(request.clone()))
        .await
        .expect("original source");
    let watch = crate::SourceWatchRepository::new(storage.clone())
        .create(SourceWatch {
            base: base(TableName::SourceWatch),
            source: Link::new(first.source_id.clone()),
            requested_uri: "https://example.invalid/fixture".into(),
            status: SourceWatchStatus::Enabled,
            interval_seconds: 3600,
            next_check_at: chrono::Utc::now(),
            last_checked_at: None,
            last_changed_at: None,
            etag: None,
            last_modified: None,
            observed_hash: Some("original-hash".into()),
            consecutive_failures: 0,
            last_error: None,
            latest_check: None,
        })
        .await
        .expect("watch");
    let principal = elephant_policy::Principal {
        space_id: space.into(),
        scopes: ["read:source", "write:source", "read:doc", "write:doc"]
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
    };
    let admission =
        elephant_auth::standing_watch::StandingWatchAdmission::trusted_local(&principal)
            .expect("explicit bounded fixture local admission");
    let current_source = crate::SourceRepository::new(storage.clone())
        .get(space, &first.source_id)
        .await
        .expect("original current source")
        .expect("accepted source");
    let records = Box::pin(
        crate::source_watch_registration::PreparedSourceWatchRegistration::prepare(
            &storage,
            &current_source,
            Some(&watch),
            watch.clone(),
        ),
    )
    .await
    .expect("actual original source/document authorization inputs");
    let registration = Box::pin(
        crate::standing_watch_repository::StandingWatchRepository::new(storage.clone())
            .prepare_registration(&watch, &admission),
    )
    .await
    .expect("explicit standing registration");
    let (_, identity) = Box::pin(records.register(&storage, &registration))
        .await
        .expect("atomic Source/Docs/Watch/private delegation registration");
    let queue = WorkItemRepository::new(storage.clone());
    let mut work = item("refresh_source_watch", base(TableName::WorkItem));
    work.payload = serde_json::json!({
        "watch_id": watch.base.id,
        "standing_watch_delegation": identity,
        "watch_intent_fingerprint": crate::source_watch_producer::watch_intent_fingerprint(
            &watch, "watch-service",
        ).expect("original semantic watch intent"),
    });
    let work = queue.create(work).await.expect("actual watch intent");
    let (work, run) = Box::pin(claimed_run(&storage, &work)).await;
    let attempt = crate::work_attempt_repository::fence_from_claim(&work).expect("original claim");
    let producer = Box::pin(PreparedSourceWatchProducer::capture(
        &storage,
        &watch,
        "watch-service",
        &attempt,
        &run.base.id,
    ))
    .await
    .expect("sealed original producer");
    request.source.hash = "changed-hash".into();
    request.revision_id = "watch-r2".into();
    request.expected = SourcePrecondition::Generation(first.source_generation);
    request.idempotency_key = "original-watch-accept".into();
    request.request_fingerprint = "fetched-original-input".into();
    request.authorized = publications
        .authorization_snapshot(space, &first.source_id)
        .await
        .expect("original source authority");
    let accepted =
        Box::pin(publications.accept_with_watch_producer(request.clone(), &producer, None, None))
            .await
            .expect("source acceptance precedes queue");
    let publication = PublicationFence {
        source_id: accepted.source_id.clone(),
        generation: accepted.source_generation.clone(),
        revision_id: "watch-r2".into(),
        operation_id: accepted.operation_id.clone(),
    };
    let source = publications
        .revision(space, &publication.source_id, &publication.revision_id)
        .await
        .expect("accepted revision")
        .expect("present")
        .source;
    let mut child_base = source.base.clone();
    child_base.id = RecordId::new_v4(TableName::WorkItem);
    let mut child = item("triage_source", child_base);
    child.payload = serde_json::json!({"source_id":source.base.id.to_string(),
        "policy_version":"source-publication-v1","_publication":publication});
    let before_head = publications
        .head(space, &source.base.id)
        .await
        .expect("head");
    let origins = WorkProducerRepository::new(storage.clone());
    if cancel {
        queue
            .update_running_for_worker(
                space,
                &work.base.id,
                &attempt.lease_token,
                WorkItemStatus::Cancelled,
                None,
            )
            .await
            .expect("cancel original")
            .expect("cancelled");
        // A receipt retry is read-only even when its original worker is stale.
        let replay =
            Box::pin(publications.accept_with_watch_producer(request, &producer, None, None))
                .await
                .expect("read-only original receipt");
        assert_eq!(replay, accepted);
    }
    let before_items = queue
        .list(space, 100, None)
        .await
        .expect("queue before attempted handoff");
    let result =
        Box::pin(origins.enqueue_watch_source(&producer, &source, &publication, child.clone()))
            .await;
    if cancel {
        assert!(
            matches!(result, Err(StorageError::Conflict(message)) if message.contains("stale_attempt"))
        );
        assert_eq!(
            serde_json::to_value(queue.list(space, 100, None).await.expect("queue")).expect("JSON"),
            serde_json::to_value(before_items).expect("JSON")
        );
    } else {
        let queued = result.expect("original attempt handoff");
        assert_eq!(
            queued.payload["_publication"],
            serde_json::to_value(&publication).expect("fence")
        );
        assert!(
            matches!(origins.origin_for_work(&queued).await.expect("private accounting"),
            WorkProducerOrigin::Child { parent_work_item_id, original_parent_work_run_id, original_parent_attempt,
                wait_for_parent_completion: false, .. }
            if parent_work_item_id == work.base.id && original_parent_work_run_id == run.base.id && original_parent_attempt == attempt)
        );
        let replay =
            Box::pin(origins.enqueue_watch_source(&producer, &source, &publication, child))
                .await
                .expect("same guarded enqueue");
        assert_eq!(replay.base.id, queued.base.id);
        assert_eq!(
            queue.list(space, 100, None).await.expect("queue").len(),
            before_items.len() + 1
        );
    }
    assert_eq!(
        publications
            .head(space, &source.base.id)
            .await
            .expect("unchanged accepted head"),
        before_head
    );
    assert_eq!(
        publications
            .operation(space, &accepted.operation_id)
            .await
            .expect("original receipt"),
        Some(accepted)
    );
}

#[tokio::test]
async fn embedded_original_watch_enqueues_fenced_source_child() {
    Box::pin(contract(true, false)).await;
}
#[tokio::test]
async fn native_original_watch_enqueues_fenced_source_child() {
    Box::pin(contract(false, false)).await;
}
#[tokio::test]
async fn embedded_cancelled_watch_cannot_enqueue_after_acceptance() {
    Box::pin(contract(true, true)).await;
}
#[tokio::test]
async fn native_cancelled_watch_cannot_enqueue_after_acceptance() {
    Box::pin(contract(false, true)).await;
}
