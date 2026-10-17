//! Real saved rows and original claims for source-producer admission.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use super::tests::{client, request};
use super::*;
use crate::canonical_commit::CanonicalRepository;
use crate::source_producer::PreparedSourceProducer;
use crate::{Repository, SpaceConfigRepository};
use elephant_types::publication::{PublicationFence, SourceProducerOrigin};
use elephant_types::records::base::BaseFields;
use elephant_types::records::{
    ArtifactStatus, ReprocessJob, WorkArtifact, WorkItem, WorkItemStatus, WorkRun,
};
use elephant_types::Link;

struct ProducerFixture {
    client: StorageClient,
    request: AcceptSourceRevision,
    job: ReprocessJob,
    selection: WorkArtifact,
    intent: WorkArtifact,
    attempt: elephant_types::publication::AttemptFence,
    configuration: elephant_types::provider_binding::SpaceConfigToken,
}
fn fixture_base(table: TableName) -> BaseFields {
    BaseFields::new(
        RecordId::new_v4(table),
        "business-a".into(),
        "producer-fixture".into(),
    )
}
#[expect(
    clippy::too_many_lines,
    reason = "Keep the complete fault and recovery sequence together with its original assertions."
)]
async fn fixture(embedded: bool) -> ProducerFixture {
    let client = client(embedded).await;
    if !embedded {
        for schema in [
            include_str!("../../migrations/sql/032_work_recovery.surql"),
            include_str!("../../migrations/sql/039_private_reference_fence.surql"),
            include_str!("../../migrations/sql/041_work_producer_origin.surql"),
        ] {
            client.query_checked(schema).await.unwrap();
        }
    }
    let configs = SpaceConfigRepository::new(client.clone());
    let old = configs
        .get_by_space_id("business-a")
        .await
        .unwrap()
        .unwrap();
    let mut config = old.clone();
    config.processing = Some(elephant_types::provider_binding::SpaceProcessingConfig {
        generation: 1,
        index_generation: 0,
        policy_id: "producer-policy".into(),
        connections: std::collections::BTreeMap::default(),
        profiles: std::collections::BTreeMap::default(),
        bindings: std::collections::BTreeMap::default(),
        active_embedding_profile_id: None,
        pending_reindex: None,
        completed_embedding_reindex: None,
    });
    let config = configs.replace_authorized(&old, config).await.unwrap();
    let configuration = elephant_types::provider_binding::SpaceConfigToken {
        space_id: "business-a".into(),
        config_id: config.base.id.to_string(),
        generation: 1,
        policy_id: "producer-policy".into(),
    };
    let repo = SourcePublicationRepository::new(client.clone());
    let id = RecordId::new_v4(TableName::Source);
    let accepted = repo
        .accept(request(&id, "producer-r1", SourcePrecondition::Absent))
        .await
        .unwrap();
    let mut request = request(
        &id,
        "producer-r2",
        SourcePrecondition::Generation(accepted.source_generation),
    );
    request.authorized = repo
        .authorization_snapshot("business-a", &id)
        .await
        .unwrap();
    let job = ReprocessJob {
        base: fixture_base(TableName::ReprocessJob),
        name: "original private fixture".into(),
        selection_criteria: elephant_types::records::SelectionCriteria::default(),
        target_stages: elephant_types::records::reprocess::default_reprocess_target_stages(),
        status: elephant_types::records::reprocess::ReprocessStatus::Running,
        progress: elephant_types::records::reprocess::ReprocessProgress::default(),
        priority: 0,
        budget_limit_euros: Some(4.0),
        max_sources_per_day: None,
        started_at: Some(Utc::now()),
        completed_at: None,
        error: None,
        reason: None,
    };
    let job = crate::ReprocessJobRepository::new(client.clone())
        .create(job)
        .await
        .unwrap();
    // This oracle exercises admission/storage, not provider dispatch. A real
    // claimed item and its actual WorkRun supply the original attempt linkage.
    let queue = crate::WorkItemRepository::new(client.clone());
    let item = queue
        .create(WorkItem {
            base: fixture_base(TableName::WorkItem),
            kind: "source_producer_fixture".into(),
            payload: serde_json::json!({}),
            status: WorkItemStatus::Runnable,
            priority: 0,
            idempotency_key: "producer-fixture".into(),
            attempts: 0,
            lease_expires_at: None,
            leased_by: None,
            run_after: None,
            last_error: None,
        })
        .await
        .unwrap();
    let claimed = queue
        .claim_with_lease(
            "business-a",
            &item.base.id,
            "fixture-worker",
            Utc::now() + chrono::Duration::minutes(5),
        )
        .await
        .unwrap()
        .unwrap();
    let attempt = crate::work_attempt_repository::fence_from_claim(&claimed).unwrap();
    let run = crate::WorkRunRepository::new(client.clone())
        .create(WorkRun {
            base: fixture_base(TableName::WorkRun),
            work_item: Link::new(item.base.id),
            attempt: claimed.attempts,
            started_at: Utc::now(),
            completed_at: None,
            success: false,
            error: None,
            metrics: elephant_types::records::work::WorkRunMetrics::default(),
            worker_id: "fixture-worker".into(),
            worker_version: "fixture".into(),
            model_version: None,
            prompt_id: None,
            config_hash: None,
        })
        .await
        .unwrap();
    let artifact = |kind: &str, data| WorkArtifact {
        base: fixture_base(TableName::WorkArtifact),
        work_run: Link::new(run.base.id.clone()),
        artifact_type: kind.into(),
        data,
        status: ArtifactStatus::Validated,
        validation_errors: None,
        validated_at: Some(Utc::now()),
        committed_at: None,
    };
    let intent=crate::WorkArtifactRepository::new(client.clone()).create(artifact("reprocess_source_intent/v1",serde_json::json!({"job_id":job.base.id,"source":request.source,"authorized":request.authorized,"expected":request.expected,"revision_id":request.revision_id,"idempotency_key":request.idempotency_key,"request_fingerprint":request.request_fingerprint,"configuration":configuration}))).await.unwrap();
    let selection=crate::WorkArtifactRepository::new(client.clone()).create(artifact("reprocess_selection/v1",serde_json::json!({"job_id":job.base.id,"source_count":1,"source_artifact_digest":"coordinator-verified-fixture-digest","configuration":configuration}))).await.unwrap();
    ProducerFixture {
        client,
        request,
        job,
        selection,
        intent,
        attempt,
        configuration,
    }
}
async fn prepare(fixture: &ProducerFixture, job: &ReprocessJob) -> PreparedSourceProducer {
    let epoch = CanonicalRepository::new(fixture.client.clone())
        .epoch("business-a")
        .await
        .unwrap();
    PreparedSourceProducer::from_verified_reprocess_selection(
        &epoch,
        job,
        &fixture.selection,
        &fixture.intent,
        std::slice::from_ref(&fixture.intent),
    )
    .unwrap()
}
fn publication(receipt: &OperationReceipt) -> PublicationFence {
    PublicationFence {
        source_id: receipt.source_id.clone(),
        generation: receipt.source_generation.clone(),
        revision_id: receipt.desired_revision.clone().unwrap(),
        operation_id: receipt.operation_id.clone(),
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
async fn producer_is_atomic_and_original_restriction_survives_retry(embedded: bool) {
    let fixture = fixture(embedded).await;
    let repo = SourcePublicationRepository::new(fixture.client.clone());
    let producer = prepare(&fixture, &fixture.job).await;
    let receipt = repo
        .accept_with_source_producer(
            fixture.request.clone(),
            &producer,
            Some(&fixture.attempt),
            Some(&fixture.configuration),
            None,
        )
        .await
        .unwrap();
    let origin = repo
        .source_operation_producer("business-a", &publication(&receipt))
        .await
        .unwrap();
    assert!(
        matches!(&origin,SourceProducerOrigin::Reprocess{job_id,currency_restricted:true,original_attempt:Some(attempt),..} if job_id==&fixture.job.base.id && attempt==&fixture.attempt)
    );
    assert!(
        repo.accept(fixture.request.clone()).await.is_err(),
        "replay cannot silently become direct intake"
    );
    let mut changed = fixture.job.clone();
    changed.budget_limit_euros = None;
    let changed = crate::ReprocessJobRepository::new(fixture.client.clone())
        .update("business-a", &changed.base.id, changed.clone())
        .await
        .unwrap();
    let retry = prepare(&fixture, &changed).await;
    assert_eq!(
        repo.accept_with_source_producer(
            fixture.request.clone(),
            &retry,
            Some(&fixture.attempt),
            Some(&fixture.configuration),
            None
        )
        .await
        .unwrap(),
        receipt
    );
    assert_eq!(
        repo.source_operation_producer("business-a", &publication(&receipt))
            .await
            .unwrap(),
        origin,
        "mutable None cannot rewrite the original finite restriction"
    );
    let mut wrong = publication(&receipt);
    wrong.generation = "foreign-generation".into();
    assert!(repo
        .source_operation_producer("business-a", &wrong)
        .await
        .is_err());
    let mut missing = publication(&receipt);
    missing.operation_id = "missing".into();
    assert_eq!(
        repo.source_operation_producer("business-a", &missing)
            .await
            .unwrap(),
        SourceProducerOrigin::UnknownOriginal
    );
}
#[tokio::test]
#[expect(
    clippy::large_futures,
    reason = "Keep the conformance helper's original await and allocation shape."
)]
async fn embedded_source_producer_is_atomic_and_sticky() {
    producer_is_atomic_and_original_restriction_survives_retry(true).await;
}
#[tokio::test]
#[expect(
    clippy::large_futures,
    reason = "Keep the conformance helper's original await and allocation shape."
)]
async fn surreal_source_producer_is_atomic_and_sticky() {
    producer_is_atomic_and_original_restriction_survives_retry(false).await;
}

#[expect(
    clippy::large_futures,
    reason = "Keep the conformance helper's original await and allocation shape."
)]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture, repository, and mutation owners alive through the original conformance phase."
)]
async fn changed_original_selection_cannot_admit_a_source(embedded: bool) {
    let fixture = fixture(embedded).await;
    let repo = SourcePublicationRepository::new(fixture.client.clone());
    let producer = prepare(&fixture, &fixture.job).await;
    let before = repo
        .head("business-a", &fixture.request.source.base.id)
        .await
        .unwrap();
    let mut changed = fixture.intent.clone();
    changed.data["revision_id"] = "different-saved-intent".into();
    crate::WorkArtifactRepository::new(fixture.client.clone())
        .update("business-a", &changed.base.id, changed.clone())
        .await
        .unwrap();
    assert!(repo
        .accept_with_source_producer(
            fixture.request.clone(),
            &producer,
            Some(&fixture.attempt),
            Some(&fixture.configuration),
            None
        )
        .await
        .is_err());
    assert_eq!(
        repo.head("business-a", &fixture.request.source.base.id)
            .await
            .unwrap(),
        before
    );
    assert!(repo
        .operation_by_idempotency_key("business-a", &fixture.request.idempotency_key)
        .await
        .unwrap()
        .is_none());
    assert!(repo
        .revision(
            "business-a",
            &fixture.request.source.base.id,
            &fixture.request.revision_id
        )
        .await
        .unwrap()
        .is_none());
    // A fresh epoch cannot repair an altered private source intent snapshot.
    let stale = prepare(&fixture, &fixture.job).await;
    assert!(repo
        .accept_with_source_producer(
            fixture.request.clone(),
            &stale,
            Some(&fixture.attempt),
            Some(&fixture.configuration),
            None
        )
        .await
        .is_err());
}
#[tokio::test]
#[expect(
    clippy::large_futures,
    reason = "Keep the conformance helper's original await and allocation shape."
)]
async fn embedded_source_producer_saved_intent_is_guarded() {
    changed_original_selection_cannot_admit_a_source(true).await;
}
#[tokio::test]
#[expect(
    clippy::large_futures,
    reason = "Keep the conformance helper's original await and allocation shape."
)]
async fn surreal_source_producer_saved_intent_is_guarded() {
    changed_original_selection_cannot_admit_a_source(false).await;
}

#[test]
fn missing_private_producer_field_remains_unknown() {
    let input = request(
        &RecordId::new_v4(TableName::Source),
        "old",
        SourcePrecondition::Absent,
    );
    let operation = StoredOperation {
        predecessor_publication: None,
        receipt: OperationReceipt {
            space_id: "business-a".into(),
            operation_id: "old-operation".into(),
            source_id: input.source.base.id.clone(),
            source_generation: "old-generation".into(),
            desired_revision: Some("old".into()),
            published_revision: None,
            state: OperationState::Accepted,
            accepted_at: Utc::now(),
            completed_at: None,
            error_code: None,
        },
        request_fingerprint: input.request_fingerprint.clone(),
        input_fingerprint: "old-input".into(),
        authorization: SourceAuthorizationEnvelope::from(&input.source),
        contact_digest: None,
        producer_origin: SourceProducerOrigin::Direct,
    };
    let mut value = serde_json::to_value(&operation).unwrap();
    value.as_object_mut().unwrap().remove("producer_origin");
    let historical: StoredOperation = serde_json::from_value(value).unwrap();
    assert_eq!(
        historical.producer_origin,
        SourceProducerOrigin::UnknownOriginal
    );
    assert!(!crate::source_producer::same_producer_intent(
        &historical.producer_origin,
        &SourceProducerOrigin::Direct
    ));
    let public_receipt = serde_json::to_value(&operation.receipt).unwrap();
    assert!(public_receipt.get("producer_origin").is_none());
    assert!(public_receipt.get("job_id").is_none());
}

#[expect(
    clippy::large_futures,
    reason = "Keep the conformance helper's original await and allocation shape."
)]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture, repository, and mutation owners alive through the original conformance phase."
)]
#[expect(
    clippy::too_many_lines,
    reason = "Keep the complete fault and recovery sequence together with its original assertions."
)]
async fn every_selection_member_and_membership_are_guarded(embedded: bool) {
    let fixture = fixture(embedded).await;
    let artifacts = crate::WorkArtifactRepository::new(fixture.client.clone());
    let repo = SourcePublicationRepository::new(fixture.client.clone());
    let canonical = CanonicalRepository::new(fixture.client.clone());
    let mut sibling = fixture.intent.clone();
    sibling.base.id = RecordId::new_v4(TableName::WorkArtifact);
    sibling.data["source"]["id"] = serde_json::json!(RecordId::new_v4(TableName::Source));
    let sibling = artifacts.create(sibling).await.unwrap();
    let mut selection = fixture.selection.clone();
    selection.data["source_count"] = 2.into();
    let selection = artifacts
        .update("business-a", &selection.base.id, selection.clone())
        .await
        .unwrap();
    let original_epoch = canonical.epoch("business-a").await.unwrap();
    let members = vec![fixture.intent.clone(), sibling.clone()];
    assert!(PreparedSourceProducer::from_verified_reprocess_selection(
        &original_epoch,
        &fixture.job,
        &selection,
        &fixture.intent,
        std::slice::from_ref(&fixture.intent)
    )
    .is_err());
    let producer = PreparedSourceProducer::from_verified_reprocess_selection(
        &original_epoch,
        &fixture.job,
        &selection,
        &fixture.intent,
        &members,
    )
    .unwrap();
    let mut changed = sibling.clone();
    changed.data["revision_id"] = "changed other selected source".into();
    let changed = artifacts
        .update("business-a", &changed.base.id, changed.clone())
        .await
        .unwrap();
    assert_ne!(canonical.epoch("business-a").await.unwrap(), original_epoch);
    assert!(repo
        .accept_with_source_producer(
            fixture.request.clone(),
            &producer,
            Some(&fixture.attempt),
            None,
            None
        )
        .await
        .is_err());
    let refreshed_epoch = canonical.epoch("business-a").await.unwrap();
    let stale = PreparedSourceProducer::from_verified_reprocess_selection(
        &refreshed_epoch,
        &fixture.job,
        &selection,
        &fixture.intent,
        &members,
    )
    .unwrap();
    assert!(
        repo.accept_with_source_producer(
            fixture.request.clone(),
            &stale,
            Some(&fixture.attempt),
            None,
            None
        )
        .await
        .is_err(),
        "recapturing only epoch cannot repair a changed other-source snapshot"
    );
    let members = vec![fixture.intent.clone(), changed.clone()];
    let mut phantom = changed;
    phantom.base.id = RecordId::new_v4(TableName::WorkArtifact);
    artifacts.create(phantom).await.unwrap();
    let refreshed_epoch = canonical.epoch("business-a").await.unwrap();
    let stale = PreparedSourceProducer::from_verified_reprocess_selection(
        &refreshed_epoch,
        &fixture.job,
        &selection,
        &fixture.intent,
        &members,
    )
    .unwrap();
    assert!(
        repo.accept_with_source_producer(
            fixture.request.clone(),
            &stale,
            Some(&fixture.attempt),
            None,
            None
        )
        .await
        .is_err(),
        "same-transaction job membership refuses an uncounted source intent"
    );
    assert!(repo
        .operation_by_idempotency_key("business-a", &fixture.request.idempotency_key)
        .await
        .unwrap()
        .is_none());
}
#[tokio::test]
#[expect(
    clippy::large_futures,
    reason = "Keep the conformance helper's original await and allocation shape."
)]
async fn embedded_source_producer_guards_all_selected_sources() {
    every_selection_member_and_membership_are_guarded(true).await;
}
#[tokio::test]
#[expect(
    clippy::large_futures,
    reason = "Keep the conformance helper's original await and allocation shape."
)]
async fn surreal_source_producer_guards_all_selected_sources() {
    every_selection_member_and_membership_are_guarded(false).await;
}

#[expect(
    clippy::large_futures,
    reason = "Keep the conformance helper's original await and allocation shape."
)]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture, repository, and mutation owners alive through the original conformance phase."
)]
async fn private_origin_bookkeeping_preserves_prepared_epoch(embedded: bool) {
    let fixture = fixture(embedded).await;
    let canonical = CanonicalRepository::new(fixture.client.clone());
    let before = canonical.epoch("business-a").await.unwrap();
    crate::WorkItemRepository::new(fixture.client.clone())
        .create(WorkItem {
            base: fixture_base(TableName::WorkItem),
            kind: "another_actual_child".into(),
            payload: serde_json::json!({}),
            status: WorkItemStatus::Runnable,
            priority: 0,
            idempotency_key: "bookkeeping-child".into(),
            attempts: 0,
            lease_expires_at: None,
            leased_by: None,
            run_after: None,
            last_error: None,
        })
        .await
        .unwrap();
    assert_eq!(canonical.epoch("business-a").await.unwrap(),before,"new actual queue item and its private origin cannot invalidate the parent's prepared semantic decision");
    if let Some(store) = fixture.client.embedded_store() {
        store
            .mutate(|tx| {
                for (key, mut row) in tx.scan_extension_rows("work_producer_origin")? {
                    row["read_nonce"] = "original parent read".into();
                    tx.replace_extension_row("work_producer_origin", &key, row)?;
                }
                Ok(())
            })
            .await
            .unwrap();
    } else {
        fixture.client.query_checked("UPDATE work_producer_origin SET read_nonce='original parent read' WHERE space_id='business-a';").await.unwrap();
    }
    assert_eq!(canonical.epoch("business-a").await.unwrap(), before);
    if let Some(store) = fixture.client.embedded_store() {
        store
            .mutate(|tx| {
                let (key, mut row) = tx
                    .scan_extension_rows("work_producer_origin")?
                    .into_iter()
                    .next()
                    .unwrap();
                row["payload"] = "{}".into();
                tx.replace_extension_row("work_producer_origin", &key, row)?;
                Ok(())
            })
            .await
            .unwrap();
    } else {
        fixture
            .client
            .query_checked(
                "UPDATE work_producer_origin SET payload='{}' WHERE space_id='business-a';",
            )
            .await
            .unwrap();
    }
    assert_ne!(
        canonical.epoch("business-a").await.unwrap(),
        before,
        "actual producer semantics remain fenced"
    );
}
#[tokio::test]
#[expect(
    clippy::large_futures,
    reason = "Keep the conformance helper's original await and allocation shape."
)]
async fn embedded_producer_queue_bookkeeping_keeps_original_planning_epoch() {
    private_origin_bookkeeping_preserves_prepared_epoch(true).await;
}
#[tokio::test]
#[expect(
    clippy::large_futures,
    reason = "Keep the conformance helper's original await and allocation shape."
)]
async fn surreal_producer_queue_bookkeeping_keeps_original_planning_epoch() {
    private_origin_bookkeeping_preserves_prepared_epoch(false).await;
}
