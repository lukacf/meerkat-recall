//! Actual signed registration, original queued execution and physical-dispatch
//! preflight. No provider, synthetic private descriptor or source completion.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use super::*;
use elephant_auth::standing_watch::StandingWatchAdmission;
use elephant_auth::{Claims, LegacyVerifier};
use elephant_policy::{Clearance, Principal};
use elephant_storage::source_publication::{AcceptSourceRevision, SourcePublicationRepository};
use elephant_storage::source_watch_registration::PreparedSourceWatchRegistration;
use elephant_storage::space_grant_repository::SpaceGrantRepository;
use elephant_storage::standing_watch_repository::StandingWatchRepository;
use elephant_storage::{SpaceConfigRepository, WorkItemRepository};
use elephant_types::publication::{AttemptFence, SourcePrecondition};
use elephant_types::records::{SourceWatch, SourceWatchStatus, SpaceConfig, SubjectsState};
use elephant_types::space_grant::SpaceGrant;
use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use std::collections::HashSet;

const SPACE: &str = "standing-dispatch-inputs";
const SIGNING_KEY: &[u8] = b"standing-dispatch-fixture-only";

fn base(table: TableName) -> BaseFields {
    let mut base = BaseFields::new(
        RecordId::new_v4(table),
        SPACE.into(),
        "original-owner".into(),
    );
    base.security.policy_id = "dispatch-policy".into();
    base.security.level = 3;
    base.subjects_state = SubjectsState::Complete;
    base
}

fn admission(grant: &SpaceGrant) -> StandingWatchAdmission {
    let principal = Principal {
        space_id: SPACE.into(),
        scopes: ["read:source", "write:source", "read:doc", "write:doc"]
            .into_iter()
            .map(str::to_owned)
            .collect(),
        clearances: vec![Clearance {
            policy_id: "dispatch-policy".into(),
            max_level: 3,
            labels: HashSet::new(),
            handling_allow: HashSet::new(),
        }],
        subject_allowlist: None,
        purpose: HashSet::from(["watch-refresh".into()]),
    };
    let claims = Claims {
        sub: grant.subject.clone(),
        exp: Utc::now().timestamp() + 3600,
        iat: Utc::now().timestamp(),
        iss: None,
        aud: None,
        space_id: Some(SPACE.into()),
        spaces: Some(vec![SPACE.into()]),
        bootstrap_space: None,
        scopes: principal.scopes.iter().cloned().collect(),
        security_level: 0,
        security_labels: vec![],
        clearances: principal.clearances.clone(),
        subject_allowlist: None,
        purpose: principal.purpose.iter().cloned().collect(),
    };
    let token = encode(
        &Header::default(),
        &claims,
        &EncodingKey::from_secret(SIGNING_KEY),
    )
    .expect("sign actual fixture claims");
    let caller = LegacyVerifier {
        secret: "standing-dispatch-fixture-only",
        algorithm: Algorithm::HS256,
        issuer: None,
        audience: None,
        mappings: None,
    }
    .verify(&token)
    .expect("verify signature");
    StandingWatchAdmission::verified(&caller, &principal, grant).expect("bounded signed admission")
}

pub(super) async fn claim_run(
    storage: &StorageClient,
    id: &RecordId,
) -> (WorkItem, elephant_types::records::WorkRun, AttemptFence) {
    use elephant_types::records::{WorkRun, WorkRunMetrics};
    let work = WorkItemRepository::new(storage.clone())
        .claim_with_lease(
            SPACE,
            id,
            "standing-oracle",
            Utc::now() + chrono::Duration::minutes(5),
        )
        .await
        .unwrap()
        .unwrap();
    let attempt = elephant_storage::work_attempt_repository::fence_from_claim(&work).unwrap();
    let mut metrics = WorkRunMetrics::default();
    metrics.custom.insert(
        "attempt_fence".into(),
        serde_json::to_value(&attempt).unwrap(),
    );
    let run = elephant_storage::WorkRunRepository::new(storage.clone())
        .create(WorkRun {
            base: base(TableName::WorkRun),
            work_item: Link::new(work.base.id.clone()),
            attempt: work.attempts,
            started_at: Utc::now(),
            completed_at: None,
            success: false,
            error: None,
            metrics,
            worker_id: attempt.lease_token.clone(),
            worker_version: "standing-oracle".into(),
            model_version: None,
            prompt_id: None,
            config_hash: None,
        })
        .await
        .unwrap();
    (work, run, attempt)
}

pub(super) async fn fixture(
    embedded: bool,
) -> (
    StorageClient,
    Source,
    WorkItem,
    elephant_types::records::WorkRun,
    SpaceGrant,
) {
    let storage = if embedded {
        Box::pin(StorageClient::new_embedded()).await.unwrap()
    } else {
        let storage = Box::pin(StorageClient::new_memory()).await.unwrap();
        Box::pin(elephant_migrations::apply_schema(&storage))
            .await
            .unwrap();
        storage
    };
    SpaceConfigRepository::new(storage.clone())
        .create(SpaceConfig {
            base: base(TableName::SpaceConfig),
            embedding_provider: "unconfigured".into(),
            embedding_model: "unconfigured".into(),
            embedding_dims: 1,
            processing: None,
            purpose: None,
            skill: None,
        })
        .await
        .unwrap();
    let mut source = Source::new(
        base(TableName::Source),
        SourceType::Web,
        "original-input".into(),
    );
    source.uri = Some("https://example.invalid/standing-dispatch".into());
    let accepted = Box::pin(SourcePublicationRepository::new(storage.clone()).accept(
        AcceptSourceRevision {
            source,
            revision_id: "original-input".into(),
            expected: SourcePrecondition::Absent,
            idempotency_key: "original-independent-source".into(),
            request_fingerprint: "original-input".into(),
            reactivate: false,
            recreate_after_erasure: false,
            authorized: None,
            authorized_document: None,
        },
    ))
    .await
    .unwrap();
    // Acceptance owns the live timestamp; the immutable accepted snapshot is
    // not a substitute for the exact current row authorized for registration.
    let source = SourceRepository::new(storage.clone())
        .get(SPACE, &accepted.source_id)
        .await
        .unwrap()
        .unwrap();
    let watch = SourceWatch {
        base: base(TableName::SourceWatch),
        source: Link::new(source.base.id.clone()),
        requested_uri: source.uri.clone().unwrap(),
        status: SourceWatchStatus::Enabled,
        interval_seconds: 300,
        next_check_at: Utc::now(),
        last_checked_at: None,
        last_changed_at: None,
        etag: None,
        last_modified: None,
        observed_hash: None,
        consecutive_failures: 0,
        last_error: None,
        latest_check: None,
    };
    let grant = SpaceGrantRepository::new(storage.clone())
        .replace(
            SpaceGrant {
                space_id: SPACE.into(),
                subject: "original-signed-dispatch-owner".into(),
                generation: String::new(),
                active: true,
                scopes: ["read:source", "write:source", "read:doc", "write:doc"]
                    .into_iter()
                    .map(str::to_owned)
                    .collect(),
                updated_by: "administrator".into(),
                updated_at: Utc::now(),
            },
            None,
        )
        .await
        .unwrap();
    let authority = StandingWatchRepository::new(storage.clone());
    let registration = Box::pin(authority.prepare_registration(&watch, &admission(&grant)))
        .await
        .unwrap();
    let records = Box::pin(PreparedSourceWatchRegistration::prepare(
        &storage,
        &source,
        None,
        watch.clone(),
    ))
    .await
    .unwrap();
    Box::pin(records.register(&storage, &registration))
        .await
        .unwrap();
    let captured = Box::pin(authority.capture(&watch, registration.identity()))
        .await
        .unwrap();
    let records = Box::pin(PreparedSourceWatchRegistration::prepare(
        &storage,
        &source,
        Some(&watch),
        watch.clone(),
    ))
    .await
    .unwrap();
    let work = Box::pin(
        WorkItemRepository::new(storage.clone())
            .enqueue_source_watch_refresh(&records, &captured, None, 1),
    )
    .await
    .unwrap();
    let (work, run, _) = claim_run(&storage, &work.base.id).await;
    (storage, source, work, run, grant)
}

async fn original_current_authority(embedded: bool) {
    let (storage, source, work, mut run, grant) = Box::pin(fixture(embedded)).await;
    Box::pin(guard_physical_dispatch(&storage, &work))
        .await
        .unwrap();
    let mut child = work.clone();
    child.base.id = RecordId::new_v4(TableName::WorkItem);
    child.kind = "extract_assertions".into();
    child.payload = serde_json::json!({"original_input": source.base.id.to_string()});
    child.status = elephant_types::records::WorkItemStatus::Runnable;
    child.idempotency_key = "actual-original-watch-child".into();
    child.attempts = 0;
    child.lease_expires_at = None;
    child.leased_by = None;
    let child = Box::pin(
        elephant_storage::work_producer_repository::WorkProducerRepository::new(storage.clone())
            .enqueue_successor(&work, &run, child, false),
    )
    .await
    .unwrap();
    let (child, _, _) = claim_run(&storage, &child.base.id).await;
    Box::pin(guard_physical_dispatch(&storage, &child))
        .await
        .unwrap();
    let original_attempt =
        elephant_storage::work_attempt_repository::fence_from_claim(&work).unwrap();
    WorkItemRepository::new(storage.clone())
        .update_running_for_worker(
            SPACE,
            &work.base.id,
            &original_attempt.lease_token,
            elephant_types::records::WorkItemStatus::Done,
            None,
        )
        .await
        .unwrap()
        .expect("complete the actual original producer lease");
    run.completed_at = Some(Utc::now());
    run.success = true;
    elephant_storage::WorkRunRepository::new(storage.clone())
        .update(SPACE, &run.base.id.clone(), run)
        .await
        .unwrap();
    // A completed producer retains its execution ceiling; no fresh claim is
    // adopted merely to allow its already accepted child to finish.
    Box::pin(guard_physical_dispatch(&storage, &child))
        .await
        .unwrap();

    // The delegation remains valid; the actual Source input moves outside its
    // original ceiling. Grant validity alone must not reach provider dispatch.
    let sources = SourceRepository::new(storage.clone());
    let mut restricted = source.clone();
    restricted.base.security.level = 4;
    sources
        .update(SPACE, &source.base.id, restricted)
        .await
        .unwrap();
    assert!(Box::pin(guard_physical_dispatch(&storage, &child))
        .await
        .is_err());
    sources
        .update(SPACE, &source.base.id, source.clone())
        .await
        .unwrap();
    Box::pin(guard_physical_dispatch(&storage, &child))
        .await
        .unwrap();

    let mut revoked = grant.clone();
    revoked.active = false;
    SpaceGrantRepository::new(storage.clone())
        .replace(revoked, Some(&grant.generation))
        .await
        .unwrap();
    assert!(Box::pin(guard_physical_dispatch(&storage, &child))
        .await
        .is_err());

    // A separately admitted root has its own real private authority and budget.
    // Selecting the same source type does not inherit an old Watch execution.
    let jobs = ReprocessJobRepository::new(storage.clone());
    let job = jobs
        .create_with_work(ReprocessJob {
            base: base(TableName::ReprocessJob),
            name: "new independently admitted intent".into(),
            selection_criteria: SelectionCriteria {
                source_types: vec!["web".into()],
                ..Default::default()
            },
            target_stages: vec![ReprocessTargetStage::ReMaintain],
            status: ReprocessStatus::Pending,
            progress: ReprocessProgress::default(),
            priority: 1,
            budget_limit_euros: None,
            max_sources_per_day: None,
            started_at: None,
            completed_at: None,
            error: None,
            reason: None,
        })
        .await
        .unwrap();
    let id = jobs.ensure_work(&job).await.unwrap();
    let (independent, _, attempt) = claim_run(&storage, &id).await;
    Box::pin(guard_physical_dispatch(&storage, &independent))
        .await
        .unwrap();
    WorkItemRepository::new(storage.clone())
        .update_running_for_worker(
            SPACE,
            &id,
            &attempt.lease_token,
            elephant_types::records::WorkItemStatus::Done,
            None,
        )
        .await
        .unwrap()
        .expect("end exactly the original captured lease");
    assert!(Box::pin(guard_physical_dispatch(&storage, &independent))
        .await
        .is_err());
}

#[tokio::test]
async fn embedded_physical_dispatch_rechecks_original_standing_and_current_source() {
    Box::pin(original_current_authority(true)).await;
}

#[tokio::test]
async fn native_physical_dispatch_rechecks_original_standing_and_current_source() {
    Box::pin(original_current_authority(false)).await;
}

async fn original_direct_work_without_epoch(embedded: bool) {
    use elephant_storage::canonical_commit::CanonicalRepository;

    let storage = if embedded {
        Box::pin(StorageClient::new_embedded()).await.unwrap()
    } else {
        let storage = Box::pin(StorageClient::new_memory()).await.unwrap();
        Box::pin(elephant_migrations::apply_schema(&storage))
            .await
            .unwrap();
        storage
    };
    // The actual retained Direct constructor seals a private origin without a
    // Source, Job or configured-space write. It does not initialize an epoch.
    let work = WorkItemRepository::new(storage.clone())
        .create(WorkItem {
            base: base(TableName::WorkItem),
            kind: "maintenance_sweep".into(),
            payload: serde_json::json!({}),
            status: elephant_types::records::WorkItemStatus::Runnable,
            priority: 1,
            idempotency_key: "source-less-original-work".into(),
            attempts: 0,
            lease_expires_at: None,
            leased_by: None,
            run_after: None,
            last_error: None,
        })
        .await
        .unwrap();
    let (original, _, _) = claim_run(&storage, &work.base.id).await;
    let canonical = CanonicalRepository::new(storage.clone());
    assert_eq!(canonical.observe_epoch(SPACE).await.unwrap(), None);
    for _ in 0..2 {
        Box::pin(guard_physical_dispatch(&storage, &original))
            .await
            .expect("original source-less execution remains valid with no epoch");
        assert_eq!(
            canonical.observe_epoch(SPACE).await.unwrap(),
            None,
            "read-only dispatch preflight must not initialize missing state"
        );
    }
    // This is the accounting/standing preflight only. Concrete provider calls
    // still require their own current selected-space configuration authority.
}

#[tokio::test]
async fn embedded_original_direct_dispatch_preserves_epoch_absence() {
    Box::pin(original_direct_work_without_epoch(true)).await;
}

#[tokio::test]
async fn native_original_direct_dispatch_preserves_epoch_absence() {
    Box::pin(original_direct_work_without_epoch(false)).await;
}

async fn original_document_membership(embedded: bool) {
    use elephant_storage::standing_dispatch_inputs::PreparedStandingDispatchInputs;
    use elephant_types::publication::{PublicationFence, SourceAuthorizationSnapshot};
    use elephant_types::records::ManualDocTemplate;

    let (storage, _, _, _, _) = Box::pin(fixture(embedded)).await;
    let mut source = Source::new(
        base(TableName::Source),
        SourceType::Manual,
        "document-bytes".into(),
    );
    source
        .set_manual_doc_template(&ManualDocTemplate {
            doc_id: RecordId::new_v4(TableName::Doc).to_string(),
            title: "Original document".into(),
            text: "Original document bytes".into(),
            sidecar_text: None,
            media_attachments: vec![],
            rendition_key: "standing-dispatch-document".into(),
            metadata: serde_json::json!({}),
        })
        .unwrap();
    let sources = SourceRepository::new(storage.clone());
    let source = sources.create(source).await.unwrap();
    let absent_head = Box::pin(PreparedStandingDispatchInputs::capture(
        &storage,
        SPACE,
        &source.base.id,
    ))
    .await
    .unwrap();
    assert_eq!(absent_head.records().count(), 1);
    absent_head.verify_current(&storage).await.unwrap();
    let accepted = Box::pin(SourcePublicationRepository::new(storage.clone()).accept(
        AcceptSourceRevision {
            source: source.clone(),
            revision_id: "original-document-input".into(),
            expected: SourcePrecondition::Absent,
            idempotency_key: "original-document-input".into(),
            request_fingerprint: "original-document-input".into(),
            reactivate: false,
            recreate_after_erasure: false,
            authorized: Some(SourceAuthorizationSnapshot::from(&source)),
            authorized_document: None,
        },
    ))
    .await
    .unwrap();
    assert!(absent_head.verify_current(&storage).await.is_err());
    let original = PublicationFence {
        source_id: accepted.source_id.clone(),
        generation: accepted.source_generation.clone(),
        revision_id: accepted.desired_revision.clone().unwrap(),
        operation_id: accepted.operation_id.clone(),
    };
    let absent_document = Box::pin(PreparedStandingDispatchInputs::capture(
        &storage,
        SPACE,
        &source.base.id,
    ))
    .await
    .unwrap();
    assert_eq!(absent_document.records().count(), 1);
    let immutable = SourcePublicationRepository::new(storage.clone())
        .revision(SPACE, &source.base.id, &original.revision_id)
        .await
        .unwrap()
        .unwrap();
    Box::pin(elephant_storage::source_intake::stage_source_revision(
        &storage,
        &immutable.source,
        &original,
    ))
    .await
    .unwrap();
    assert!(absent_document.verify_current(&storage).await.is_err());
    let present = Box::pin(PreparedStandingDispatchInputs::capture(
        &storage,
        SPACE,
        &source.base.id,
    ))
    .await
    .unwrap();
    let document = present
        .records()
        .find_map(|record| match record {
            elephant_storage::canonical_commit::CanonicalRecord::Doc(document) => {
                Some(document.clone())
            }
            _ => None,
        })
        .expect("actual staged Doc retained");
    assert_eq!(present.records().count(), 2);
    present.verify_current(&storage).await.unwrap();
    let mut restricted = (*document).clone();
    restricted.base.security.level = 4;
    DocRepository::new(storage.clone())
        .update(SPACE, &document.base.id, restricted)
        .await
        .unwrap();
    assert!(present.verify_current(&storage).await.is_err());
    assert_eq!(
        SourcePublicationRepository::new(storage.clone())
            .operation(SPACE, &accepted.operation_id)
            .await
            .unwrap()
            .unwrap(),
        accepted,
        "input observation must not fabricate source completion"
    );
}

#[tokio::test]
async fn embedded_dispatch_capture_keeps_absent_head_and_document_observations() {
    Box::pin(original_document_membership(true)).await;
}

#[tokio::test]
async fn native_dispatch_capture_keeps_absent_head_and_document_observations() {
    Box::pin(original_document_membership(false)).await;
}
