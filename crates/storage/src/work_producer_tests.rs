#![allow(clippy::expect_used, clippy::too_many_lines)]
use super::*;
use crate::{ConflictGroupRepository, WorkItemRepository, WorkRunRepository};
use elephant_types::records::reprocess::{
    ReprocessProgress, ReprocessStatus, ReprocessTargetStage, SelectionCriteria,
};
use elephant_types::records::{BaseFields, WorkItemStatus, WorkRunMetrics};
use elephant_types::Link;

#[path = "work_producer_watch_tests.rs"]
mod watch_queue;

fn item(kind: &str, base: BaseFields) -> WorkItem {
    WorkItem {
        base,
        kind: kind.into(),
        payload: serde_json::json!({}),
        status: WorkItemStatus::Runnable,
        priority: 1,
        idempotency_key: uuid::Uuid::new_v4().to_string(),
        attempts: 0,
        lease_expires_at: None,
        leased_by: None,
        run_after: None,
        last_error: None,
    }
}
fn job(budget: Option<f64>) -> ReprocessJob {
    ReprocessJob {
        base: BaseFields::new(
            RecordId::new_v4(TableName::ReprocessJob),
            "producer-space".into(),
            "private-coordinator".into(),
        ),
        name: "original intent".into(),
        selection_criteria: SelectionCriteria::default(),
        target_stages: vec![ReprocessTargetStage::ReMaintain],
        status: ReprocessStatus::Pending,
        progress: ReprocessProgress::default(),
        priority: 1,
        budget_limit_euros: budget,
        max_sources_per_day: None,
        started_at: None,
        completed_at: None,
        error: None,
        reason: None,
    }
}
#[test]
fn immutable_intents_ignore_execution_state_but_bind_semantics_and_authority() {
    let original_job = job(Some(1.0));
    let original = item(
        "stage",
        BaseFields::new(
            RecordId::new_v4(TableName::WorkItem),
            "producer-space".into(),
            "actor".into(),
        ),
    );
    let intent = work_intent_fingerprint(&original).expect("object-shaped original work intent");
    let mut claimed = original.clone();
    claimed.base.updated_at += chrono::Duration::seconds(1);
    claimed.status = WorkItemStatus::Running;
    claimed.attempts = 7;
    claimed.payload["_attempt_fence"] = serde_json::json!({"generation":7});
    assert_eq!(
        work_intent_fingerprint(&claimed).expect("claimed intent"),
        intent
    );
    claimed.payload["original_input"] = serde_json::json!("changed");
    assert_ne!(
        work_intent_fingerprint(&claimed).expect("changed payload"),
        intent
    );
    let mut changed = original.clone();
    changed.base.security.level += 1;
    assert_ne!(
        work_intent_fingerprint(&changed).expect("changed authority"),
        intent
    );
    changed = original;
    changed.base.created_by = "another actor".into();
    assert_ne!(
        work_intent_fingerprint(&changed).expect("changed actor"),
        intent
    );

    let intent = job_intent_fingerprint(&original_job).expect("object-shaped original job intent");
    let mut progressed = original_job;
    progressed.base.updated_at += chrono::Duration::seconds(1);
    progressed.status = ReprocessStatus::Running;
    progressed.budget_limit_euros = None; // sticky restriction is separate original authority
    assert_eq!(
        job_intent_fingerprint(&progressed).expect("progressed intent"),
        intent
    );
    progressed.target_stages.clear();
    assert_ne!(
        job_intent_fingerprint(&progressed).expect("changed stages"),
        intent
    );
}

async fn claimed_run(storage: &StorageClient, work: &WorkItem) -> (WorkItem, WorkRun) {
    let parent = WorkItemRepository::new(storage.clone())
        .claim_with_lease(
            &work.base.space_id,
            &work.base.id,
            "original",
            chrono::Utc::now() + chrono::Duration::minutes(5),
        )
        .await
        .expect("claim")
        .expect("owned");
    let fence =
        crate::work_attempt_repository::fence_from_claim(&parent).expect("original attempt");
    let mut metrics = WorkRunMetrics::default();
    metrics.custom.insert(
        "attempt_fence".into(),
        serde_json::to_value(&fence).expect("fence"),
    );
    let mut base = parent.base.clone();
    base.id = RecordId::new_v4(TableName::WorkRun);
    let run = WorkRunRepository::new(storage.clone())
        .create(WorkRun {
            base,
            work_item: Link::new(parent.base.id.clone()),
            attempt: parent.attempts,
            started_at: chrono::Utc::now(),
            completed_at: None,
            success: false,
            error: None,
            metrics,
            worker_id: fence.lease_token,
            worker_version: "fixture".into(),
            model_version: None,
            prompt_id: None,
            config_hash: None,
        })
        .await
        .expect("actual execution receipt");
    (parent, run)
}
async fn delete_origin(storage: &StorageClient, work: &WorkItem) {
    if let Some(store) = storage.embedded_store() {
        store
            .mutate(|tx| {
                tx.hard_delete_extension_row(
                    TABLE,
                    &origin_key(&work.base.id),
                    &work.base.space_id,
                )?;
                Ok::<(), StorageError>(())
            })
            .await
            .expect("physical origin deletion");
    } else {
        storage
            .query_bind_checked(
                "DELETE $id",
                crate::bindings! { "id" => origin_id(&work.base.id) },
            )
            .await
            .expect("physical origin deletion");
    }
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep origins, jobs, items in the original fixture scope through assertions and cleanup."
)]
async fn producer_contract(storage: StorageClient) {
    if !storage.is_embedded() {
        for schema in [
            include_str!("../../migrations/sql/031_source_publication.surql"),
            include_str!("../../migrations/sql/032_work_recovery.surql"),
            include_str!("../../migrations/sql/041_work_producer_origin.surql"),
        ] {
            storage
                .query_checked(schema)
                .await
                .expect("actual original queue and canonical epoch schema before records");
        }
        crate::reference_fence::install_native_reference_guards(&storage, &[])
            .await
            .expect("actual private and physical guard schema before records");
    }
    let origins = WorkProducerRepository::new(storage.clone());
    let jobs = crate::reprocess_job_repository::ReprocessJobRepository::new(storage.clone());
    let items = WorkItemRepository::new(storage.clone());
    for initial in [Some(1.0), None] {
        // Original intent exists before any queue handoff, including a crash gap.
        let original = jobs
            .create(job(initial))
            .await
            .expect("persist original intent");
        let mut changed = original.clone();
        changed.budget_limit_euros = if initial.is_some() { None } else { Some(1.0) };
        jobs.update(&changed.base.space_id, &changed.base.id, changed.clone())
            .await
            .expect("current policy change");
        assert!(
            matches!(origins.origin_for_job(&changed).await.expect("pre-handoff origin"), WorkProducerOrigin::ReprocessJob { original_currency_restricted, .. } if original_currency_restricted == initial.is_some())
        );
        let id = jobs
            .ensure_work(&changed)
            .await
            .expect("recover exact original intent");
        let work = items
            .get(&changed.base.space_id, &id)
            .await
            .expect("read")
            .expect("handoff");
        let origin = origins.origin_for_work(&work).await.expect("actual origin");
        verify_reprocess_origin(&origin, &changed, &work).expect("original structural intent");
        assert!(
            matches!(origin, WorkProducerOrigin::ReprocessJob { original_currency_restricted, .. } if original_currency_restricted == initial.is_some())
        );
    }
    let root = jobs
        .create_with_work(job(Some(2.0)))
        .await
        .expect("atomic root");
    let id = jobs.ensure_work(&root).await.expect("root handoff");
    let work = items
        .get(&root.base.space_id, &id)
        .await
        .expect("read")
        .expect("work");
    let (parent, run) = claimed_run(&storage, &work).await;
    let mut group_base = BaseFields::new(
        RecordId::new_v4(TableName::ConflictGroup),
        parent.base.space_id.clone(),
        "group-owner".into(),
    );
    group_base.security.level = 4;
    group_base.security.labels = vec!["group-policy".into()];
    let group = ConflictGroupRepository::new(storage.clone())
        .create(ConflictGroup {
            base: group_base,
            slot_key: "public-group-slot".into(),
            slot_type: "attribute".into(),
            claim_ids: vec![],
            resolved: false,
            resolution_rationale: None,
            detected_at: chrono::Utc::now(),
            resolved_at: None,
            adjudicated_claim_set_hash: None,
            adjudicated_at: None,
        })
        .await
        .expect("actual canonical group");
    let mut child_base = group.base.clone();
    child_base.id = RecordId::new_v4(TableName::WorkItem);
    let mut proposed = item("tm_resolve_slot", child_base);
    proposed.idempotency_key = format!("group:{}:private-run:{}", group.base.id, run.base.id);
    proposed.payload = serde_json::json!({"conflict_group_id":group.base.id.to_string(),"_predecessor_work_item":parent.base.id.to_string()});
    let child = origins
        .enqueue_group_child(&parent, &run, &group, proposed.clone(), false)
        .await
        .expect("cross-policy child");
    assert_eq!(child.base.security, group.base.security);
    assert_eq!(child.base.provenance, group.base.provenance);
    assert_eq!(
        child.payload,
        serde_json::json!({"conflict_group_id":group.base.id.to_string()})
    );
    let public = serde_json::to_string(&child).expect("public wire");
    for private in [&parent.base.id, &run.base.id, &root.base.id] {
        assert!(!public.contains(&private.to_string()), "private ID leaked");
    }
    let origin = origins.origin_for_work(&child).await.expect("private link");

    verify_parent_origin(&origin, &parent, &run).expect("original linkage");
    assert_eq!(
        origins
            .enqueue_group_child(&parent, &run, &group, proposed.clone(), false)
            .await
            .expect("exact replay"),
        child
    );
    let mut stale = group.clone();
    stale.base.security.level = 0;
    let count = items
        .list(&parent.base.space_id, 100, None)
        .await
        .expect("before")
        .len();
    assert!(origins
        .enqueue_group_child(&parent, &run, &stale, proposed.clone(), false)
        .await
        .is_err());
    assert_eq!(
        items
            .list(&parent.base.space_id, 100, None)
            .await
            .expect("after")
            .len(),
        count
    );
    let mut changed_group = group.clone();
    changed_group.resolved = true;
    ConflictGroupRepository::new(storage.clone())
        .update(&group.base.space_id, &group.base.id, changed_group)
        .await
        .expect("concurrent actual group change");
    assert!(
        origins
            .enqueue_group_child(&parent, &run, &group, proposed.clone(), false)
            .await
            .is_err(),
        "the enqueue transaction must reject a changed actual group, even for replay"
    );
    assert_eq!(
        items
            .get(&child.base.space_id, &child.base.id)
            .await
            .expect("unchanged child"),
        Some(child.clone())
    );
    ConflictGroupRepository::new(storage.clone())
        .update(&group.base.space_id, &group.base.id, group.clone())
        .await
        .expect("restore group fixture");
    let fence = crate::work_attempt_repository::fence_from_claim(&parent).expect("fence");
    let done = items
        .finish_authorized(&parent, &fence.lease_token, WorkItemStatus::Done, None)
        .await
        .expect("finish")
        .expect("owned");
    let mut completed = run.clone();
    completed.success = true;
    completed.completed_at = Some(chrono::Utc::now());
    WorkRunRepository::new(storage.clone())
        .update(&run.base.space_id, &run.base.id, completed.clone())
        .await
        .expect("complete real run");
    // Consistent snapshots require all actual claimed work to be drained.
    if storage.is_embedded() {
        let directory = tempfile::tempdir().expect("snapshot directory");
        let path = directory.path().join("producer.snapshot");
        storage
            .export_snapshot(&path)
            .await
            .expect("consistent origin snapshot");
        delete_origin(&storage, &child).await;
        assert_eq!(
            origins.origin_for_work(&child).await.expect("missing"),
            WorkProducerOrigin::Unknown
        );
        storage
            .import_snapshot(&path)
            .await
            .expect("restore through original journal barrier");
        assert_eq!(
            origins
                .origin_for_work(&child)
                .await
                .expect("restored exact link"),
            origin
        );
    }

    let (child_claim, child_run) = claimed_run(&storage, &child).await;
    let mut next_base = child.base.clone();
    next_base.id = RecordId::new_v4(TableName::WorkItem);
    let next = origins
        .enqueue_successor(&child_claim, &child_run, item("tm_commit", next_base), true)
        .await
        .expect("private grandchild");
    assert!(matches!(
        origins
            .origin_for_work(&next)
            .await
            .expect("grandchild origin"),
        WorkProducerOrigin::Child { .. }
    ));
    crate::work_retention::archive_terminal_work(
        &storage,
        chrono::Utc::now() + chrono::Duration::seconds(1),
        100,
    )
    .await
    .expect("archive");
    let archived_parent = items
        .get(&parent.base.space_id, &parent.base.id)
        .await
        .expect("archive read")
        .expect("parent");
    assert_eq!(archived_parent, done);
    let archived_run = WorkRunRepository::new(storage.clone())
        .get(&run.base.space_id, &run.base.id)
        .await
        .expect("archived run read")
        .expect("original run");
    verify_parent_origin(&origin, &archived_parent, &archived_run)
        .expect("archive does not rebind original authority");
    assert!(
        origins
            .enqueue_group_child(&parent, &run, &group, proposed.clone(), false)
            .await
            .is_err(),
        "expired parent cannot create or replay effects"
    );
    delete_origin(&storage, &child).await;
    assert_eq!(
        origins
            .origin_for_work(&child_claim)
            .await
            .expect("missing origin"),
        WorkProducerOrigin::Unknown
    );
    let mut next_base = child.base.clone();
    next_base.id = RecordId::new_v4(TableName::WorkItem);
    assert!(
        origins
            .enqueue_successor(&child_claim, &child_run, item("tm_commit", next_base), true)
            .await
            .is_err(),
        "missing ancestor cannot become direct"
    );
    assert_eq!(
        origins
            .origin_for_work(&child_claim)
            .await
            .expect("still absent"),
        WorkProducerOrigin::Unknown
    );
}

#[tokio::test]
#[expect(
    clippy::large_futures,
    reason = "Poll the original conformance future directly without changing its allocation or await shape."
)]
async fn embedded_private_origin_survives_policy_changes_children_archive_and_deletion() {
    producer_contract(StorageClient::new_embedded().await.expect("embedded")).await;
}
#[tokio::test]
#[expect(
    clippy::large_futures,
    reason = "Poll the original conformance future directly without changing its allocation or await shape."
)]
async fn native_private_origin_survives_policy_changes_children_archive_and_deletion() {
    producer_contract(StorageClient::new_memory().await.expect("native")).await;
}

#[test]
fn exact_captured_origin_payload_binds_the_whole_original_envelope() {
    let original = item(
        "stage",
        BaseFields::new(
            RecordId::new_v4(TableName::WorkItem),
            "producer-space".into(),
            "actor".into(),
        ),
    );
    let original_envelope = OriginEnvelope::new(&original, WorkProducerOrigin::Direct, &[])
        .expect("original complete control");
    let payload = original_envelope.payload().expect("original bytes");
    assert_eq!(
        verify_original_control_payload(&original, &payload).expect("same original evidence"),
        WorkProducerOrigin::Direct
    );
    let mut wrong_version = original_envelope.clone();
    wrong_version.version += 1;
    let mut wrong_space = original_envelope.clone();
    wrong_space.space_id = "foreign-space".into();
    let mut wrong_work = original_envelope.clone();
    wrong_work.work_item_id = RecordId::new_v4(TableName::WorkItem);
    let mut wrong_intent = original_envelope.clone();
    wrong_intent.intent_fingerprint = "another-intent".into();
    for prior in [wrong_version, wrong_space, wrong_work, wrong_intent] {
        assert_eq!(prior.origin, original_envelope.origin);
        assert!(
            verify_original_control_payload(&original, &prior.payload().expect("prior bytes"))
                .is_err(),
            "a valid later row with the same origin cannot validate a changed captured envelope"
        );
    }
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep storage in the original fixture scope through assertions and cleanup."
)]
async fn native_standing_origin_guard_changes_nonce_without_rotating_semantic_epoch() {
    #[derive(Deserialize)]
    struct GuardRow {
        payload: String,
        read_nonce: Option<String>,
        canonical_read_nonce: Option<String>,
    }
    use crate::standing_watch_repository::PreparedStandingSourceAuthority;
    let storage = Box::pin(StorageClient::new_memory()).await.expect("native");
    for schema in [
        include_str!("../../migrations/sql/031_source_publication.surql"),
        include_str!("../../migrations/sql/032_work_recovery.surql"),
        include_str!("../../migrations/sql/039_private_reference_fence.surql"),
        include_str!("../../migrations/sql/041_work_producer_origin.surql"),
    ] {
        storage
            .query_checked(schema)
            .await
            .expect("explicit schema");
    }
    // A real canonical creation supplies the existing semantic epoch. The
    // observed Work itself is an explicitly admitted direct queue root.
    let original_job = crate::ReprocessJobRepository::new(storage.clone())
        .create(job(Some(1.0)))
        .await
        .expect("actual canonical writer");
    let mut base = original_job.base.clone();
    base.id = RecordId::new_v4(TableName::WorkItem);
    let work = WorkItemRepository::new(storage.clone())
        .create(item("stage", base))
        .await
        .expect("actual direct root and private origin");
    let original = WorkProducerRepository::new(storage.clone())
        .read_envelope(&work)
        .await
        .expect("read original")
        .expect("original envelope");
    let proof = PreparedStandingSourceAuthority::capture_for_work(&storage, &work)
        .await
        .expect("actual aggregate");
    let epoch_id = crate::source_publication::control_thing("canonical_epoch", &work.base.space_id);
    let mut response = storage
        .query_bind_checked(
            "SELECT VALUE epoch FROM ONLY $id;",
            crate::bindings! {"id"=>epoch_id.clone()},
        )
        .await
        .expect("original semantic epoch");
    let epoch: Option<String> = response.take(0).expect("typed writer epoch");
    let epoch = epoch.expect("actual writer epoch");
    let mut previous = None;
    for _ in 0..2 {
        let mut sql = "BEGIN TRANSACTION;".to_owned();
        let mut bindings = BTreeMap::new();
        proof
            .append_guard_sql(&mut sql, &mut bindings)
            .expect("actual aggregate guards");
        sql.push_str("COMMIT TRANSACTION;");
        storage
            .query_bind_checked(&sql, bindings)
            .await
            .expect("actual guard transaction");
        let mut response = storage.query_bind_checked(
            "SELECT payload,read_nonce,canonical_read_nonce FROM ONLY $origin; SELECT VALUE epoch FROM ONLY $epoch;",
            crate::bindings! {"origin"=>origin_id(&work.base.id), "epoch"=>epoch_id.clone()},
        ).await.expect("original private payload and writer epoch");
        let row: Option<GuardRow> = response.take(0).expect("typed control");
        let row = row.expect("existing private origin");
        let after: Option<String> = response.take(1).expect("typed semantic epoch after read");
        let after = after.expect("original semantic epoch remains present");
        assert_eq!(
            row.payload,
            original.payload().expect("exact original bytes")
        );
        assert_eq!(
            after, epoch,
            "read nonce is not a semantic producer mutation"
        );
        assert!(
            row.canonical_read_nonce.is_none(),
            "no competing nonce field"
        );
        let nonce = row.read_nonce.expect("actual persisted changing write");
        assert_ne!(previous.as_ref(), Some(&nonce));
        previous = Some(nonce);
        proof
            .verify_current(&storage)
            .await
            .expect("original proof still current");
    }
}
