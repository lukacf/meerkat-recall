#![allow(clippy::expect_used)]
use super::*;
use crate::work_producer_repository::origin_id;
use crate::StorageError;
use chrono::Utc;
use elephant_types::records::reprocess::{
    ReprocessProgress, ReprocessStatus, ReprocessTargetStage, SelectionCriteria,
};
use elephant_types::records::{BaseFields, WorkItemStatus, WorkRunMetrics};
use elephant_types::{Link, TableName};
use surrealdb::sql::Thing;

fn job(budget: Option<f64>) -> ReprocessJob {
    ReprocessJob {
        base: BaseFields::new(
            RecordId::new_v4(TableName::ReprocessJob),
            "original-job-parent".into(),
            "authorized-owner".into(),
        ),
        name: "original job admission".into(),
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
async fn storage(embedded: bool) -> StorageClient {
    let storage = if embedded {
        Box::pin(StorageClient::new_embedded()).await
    } else {
        Box::pin(StorageClient::new_memory()).await
    }
    .expect("storage");
    if !embedded {
        for schema in [
            include_str!("../../migrations/sql/031_source_publication.surql"),
            include_str!("../../migrations/sql/032_work_recovery.surql"),
            include_str!("../../migrations/sql/039_private_reference_fence.surql"),
            include_str!("../../migrations/sql/041_work_producer_origin.surql"),
        ] {
            storage
                .query_checked(schema)
                .await
                .expect("explicit original schema");
        }
    }
    storage
}
async fn original_run(
    storage: &StorageClient,
    work: &WorkItem,
) -> (WorkItem, WorkRun, AttemptFence) {
    let claimed = crate::WorkItemRepository::new(storage.clone())
        .claim_with_lease(
            &work.base.space_id,
            &work.base.id,
            "actual-parent-worker",
            Utc::now() + chrono::Duration::minutes(5),
        )
        .await
        .expect("claim")
        .expect("original claim");
    let fence = fence_from_claim(&claimed).expect("actual original fence");
    let mut base = claimed.base.clone();
    base.id = RecordId::new_v4(TableName::WorkRun);
    let mut metrics = WorkRunMetrics::default();
    metrics.custom.insert(
        "attempt_fence".into(),
        serde_json::to_value(&fence).expect("original fence receipt"),
    );
    let run = crate::WorkRunRepository::new(storage.clone())
        .create(WorkRun {
            base,
            work_item: Link::new(work.base.id.clone()),
            attempt: claimed.attempts,
            started_at: Utc::now(),
            completed_at: None,
            success: false,
            error: None,
            metrics,
            worker_id: fence.lease_token.clone(),
            worker_version: "fixture".into(),
            model_version: None,
            prompt_id: None,
            config_hash: None,
        })
        .await
        .expect("actual original execution receipt");
    (claimed, run, fence)
}
async fn create_spawned(
    storage: &StorageClient,
    job: &ReprocessJob,
    parent: &PreparedReprocessParent,
) -> StorageResult<()> {
    if let Some(store) = storage.embedded_store() {
        store
            .mutate(|tx| {
                tx.create(job.clone())?;
                initialize_reprocess_intent_with_parent_tx(tx, job, Some(parent))
            })
            .await
    } else {
        let mut sql = "BEGIN TRANSACTION; CREATE $job_id CONTENT $job;\n".to_owned();
        let mut bindings = crate::bindings! {"job_id"=>crate::surreal::thing_from_record_id(&job.base.id),"job"=>crate::ReprocessJobRepository::reprocess_job_to_value(job)?};
        append_reprocess_intent_with_parent_sql(
            &mut sql,
            &mut bindings,
            "original_spawn",
            job,
            Some(parent),
        )?;
        sql.push_str("COMMIT TRANSACTION;");
        storage.query_bind_checked(&sql, bindings).await?;
        Ok(())
    }
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep storage, jobs, items, origin in the original fixture scope through assertions and cleanup."
)]
#[expect(
    clippy::too_many_lines,
    reason = "Keep this ordered conformance scenario and its failure assertions in one function."
)]
async fn original_parent_contract(embedded: bool) {
    let storage = storage(embedded).await;
    let jobs = crate::ReprocessJobRepository::new(storage.clone());
    let items = crate::WorkItemRepository::new(storage.clone());
    let origin = WorkProducerRepository::new(storage.clone());
    let root = jobs
        .create_with_work(job(Some(2.0)))
        .await
        .expect("actual independently authorized finite root");
    let root_id = jobs.ensure_work(&root).await.expect("root actual queue");
    let root_work = items
        .get(&root.base.space_id, &root_id)
        .await
        .expect("root work")
        .expect("present");
    assert!(matches!(
        origin
            .origin_for_job(&root)
            .await
            .expect("original private independent admission"),
        WorkProducerOrigin::ReprocessJob {
            execution_origin: ReprocessExecutionOrigin::Independent,
            original_currency_restricted: true,
            ..
        }
    ));
    let (parent, mut run, fence) = original_run(&storage, &root_work).await;
    let authority = PreparedReprocessParent::capture(&storage, &fence)
        .await
        .expect("sealed original parent");
    let child = job(None);
    create_spawned(&storage, &child, &authority)
        .await
        .expect("atomic spawned job with original parent");
    let inherited = origin
        .origin_for_job(&child)
        .await
        .expect("original pre-handoff private authority");
    let (execution_origin, original_currency_restricted) = match &inherited {
        WorkProducerOrigin::ReprocessJob {
            execution_origin,
            original_currency_restricted,
            ..
        } => Some((execution_origin, original_currency_restricted)),
        _ => None,
    }
    .expect("actual job origin required");
    assert!(
        !original_currency_restricted,
        "child own unbounded flag cannot erase finite ancestor"
    );
    verify_reprocess_execution_parent(execution_origin, &parent, &run)
        .expect("original exact historical parent");
    let child_id = jobs
        .ensure_work(&child)
        .await
        .expect("durable recovery closes job-to-queue gap");
    let child_work = items
        .get(&child.base.space_id, &child_id)
        .await
        .expect("child handoff")
        .expect("actual work");
    assert_eq!(
        origin
            .origin_for_work(&child_work)
            .await
            .expect("retained inherited origin"),
        inherited,
        "ensure_work cannot relabel inherited root independent"
    );

    items
        .finish_authorized(&parent, &fence.lease_token, WorkItemStatus::Done, None)
        .await
        .expect("finish original parent")
        .expect("owned outcome");
    run.completed_at = Some(Utc::now());
    run.success = true;
    crate::WorkRunRepository::new(storage.clone())
        .update(&run.base.space_id, &run.base.id.clone(), run.clone())
        .await
        .expect("persist original completed run");
    crate::work_retention::archive_terminal_work(
        &storage,
        Utc::now() + chrono::Duration::seconds(1),
        16,
    )
    .await
    .expect("actual original receipt archival");
    let archived_parent = items
        .get(&parent.base.space_id, &parent.base.id)
        .await
        .expect("historical original parent")
        .expect("retained");
    let archived_run = crate::WorkRunRepository::new(storage.clone())
        .get(&run.base.space_id, &run.base.id)
        .await
        .expect("historical original run")
        .expect("retained");
    verify_reprocess_execution_parent(execution_origin, &archived_parent, &archived_run)
        .expect("archival does not require obsolete lease to stay live");
    let proof =
        crate::standing_watch_repository::PreparedStandingSourceAuthority::capture_for_work(
            &storage,
            &child_work,
        )
        .await
        .expect("work-seeded resolver preserves inherited archived parent");
    assert_eq!(
        proof.principals().count(),
        0,
        "explicit independent ancestry has no invented Watch ceiling"
    );
    proof
        .verify_current(&storage)
        .await
        .expect("exact original private chain remains current");
    let mut substituted = child_work.clone();
    substituted.payload["unaccepted_input"] = serde_json::json!(true);
    assert!(
        crate::standing_watch_repository::PreparedStandingSourceAuthority::capture_for_work(
            &storage,
            &substituted,
        )
        .await
        .is_err(),
        "same queue identity cannot substitute original intent"
    );

    let independently_authorized = jobs
        .create_with_work(job(Some(3.0)))
        .await
        .expect("separate explicit authorized intent");
    assert!(matches!(
        origin
            .origin_for_job(&independently_authorized)
            .await
            .expect("independent private original proof"),
        WorkProducerOrigin::ReprocessJob {
            execution_origin: ReprocessExecutionOrigin::Independent,
            ..
        }
    ));
    assert!(
        PreparedReprocessParent::capture(&storage, &fence)
            .await
            .is_err(),
        "old completed original cannot mint another job"
    );

    let mut historical = serde_json::to_value(&inherited).expect("stored private origin");
    historical
        .as_object_mut()
        .expect("object")
        .remove("execution_origin");
    assert!(
        serde_json::from_value::<WorkProducerOrigin>(historical).is_err(),
        "missing old admission is Unknown, never Independent"
    );
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep storage, jobs, items in the original fixture scope through assertions and cleanup."
)]
async fn stale_creation_rolls_back(embedded: bool) {
    let storage = storage(embedded).await;
    let jobs = crate::ReprocessJobRepository::new(storage.clone());
    let items = crate::WorkItemRepository::new(storage.clone());
    let root = jobs.create_with_work(job(Some(2.0))).await.expect("root");
    let id = jobs.ensure_work(&root).await.expect("root handoff");
    let work = items
        .get(&root.base.space_id, &id)
        .await
        .expect("read root")
        .expect("root");
    let (parent, _, fence) = original_run(&storage, &work).await;
    let prepared = PreparedReprocessParent::capture(&storage, &fence)
        .await
        .expect("actual old original parent");
    items
        .finish_authorized(
            &parent,
            &fence.lease_token,
            WorkItemStatus::Failed,
            Some("original caller cancelled intent".into()),
        )
        .await
        .expect("retire claim")
        .expect("owned transition");
    let proposed = job(None);
    assert!(create_spawned(&storage, &proposed, &prepared)
        .await
        .is_err());
    assert!(jobs
        .get(&proposed.base.space_id, &proposed.base.id)
        .await
        .expect("no partial spawned job")
        .is_none());
    let queued = crate::reprocess_job_repository::intended_work(&proposed)
        .expect("original intended identity");
    assert!(items
        .get(&proposed.base.space_id, &queued.base.id)
        .await
        .expect("no partial queue")
        .is_none());
    if let Some(store) = storage.embedded_store() {
        store
            .mutate(|tx| {
                assert!(
                    read_tx(tx, &queued)?.is_none(),
                    "no surviving fabricated root authority"
                );
                Ok::<_, StorageError>(())
            })
            .await
            .expect("original private rows");
    } else {
        let mut response = storage
            .query_bind_checked(
                "SELECT VALUE id FROM $id",
                crate::bindings! {"id"=>origin_id(&queued.base.id)},
            )
            .await
            .expect("private origin physical lookup");
        let rows: Vec<Thing> = response.take(0).expect("private identity rows");
        assert!(rows.is_empty(), "no surviving private root authority");
    }
}

#[tokio::test]
async fn embedded_spawned_job_retains_original_parent_through_recovery_and_archive() {
    Box::pin(original_parent_contract(true)).await;
}
#[tokio::test]
async fn native_spawned_job_retains_original_parent_through_recovery_and_archive() {
    Box::pin(original_parent_contract(false)).await;
}
#[tokio::test]
async fn embedded_spawned_job_stale_parent_leaves_no_job_or_private_root() {
    Box::pin(stale_creation_rolls_back(true)).await;
}
#[tokio::test]
async fn native_spawned_job_stale_parent_leaves_no_job_or_private_root() {
    Box::pin(stale_creation_rolls_back(false)).await;
}
