//! Real private-origin dispatch checks; no provider is invoked.
#![allow(clippy::unwrap_used, clippy::expect_used)]
use super::*;
use elephant_storage::work_producer_repository::WorkProducerRepository;
use elephant_storage::{
    ConflictGroupRepository, SpaceConfigRepository, WorkItemRepository, WorkRunRepository,
};
use elephant_types::records::{
    ConflictGroup, SpaceConfig, WorkItemStatus, WorkRun, WorkRunMetrics,
};

#[test]
fn interleaved_source_links_preserve_all_256_original_work_nodes() {
    let mut walk = ProducerWalk::default();
    let mut original = Vec::new();
    original.push(
        walk.enter(ProducerNode::Source, "initial-source".into())
            .unwrap()
            .unwrap(),
    );
    for index in 0..256 {
        original.push(
            walk.enter(ProducerNode::Work, format!("work-{index}"))
                .unwrap()
                .unwrap(),
        );
        original.push(
            walk.enter(ProducerNode::Source, format!("source-{index}"))
                .unwrap()
                .unwrap(),
        );
    }
    assert!(walk
        .enter(ProducerNode::Work, "work-over-bound".into())
        .is_err());
    for key in original.into_iter().rev() {
        walk.leave(&key, true);
    }
    assert!(walk
        .enter(ProducerNode::Source, "initial-source".into())
        .unwrap()
        .is_none());
    assert!(walk
        .enter(ProducerNode::Work, "work-255".into())
        .unwrap()
        .is_none());
    let mut sources = ProducerWalk::default();
    for index in 0..257 {
        let key = sources
            .enter(ProducerNode::Source, format!("source-{index}"))
            .unwrap()
            .unwrap();
        sources.leave(&key, true);
    }
    assert!(sources
        .enter(ProducerNode::Source, "source-over-bound".into())
        .is_err());
}

#[test]
fn producer_cycles_refuse_and_failed_nodes_never_become_verified_replays() {
    let mut walk = ProducerWalk::default();
    let work = walk
        .enter(ProducerNode::Work, "work".into())
        .unwrap()
        .unwrap();
    let source = walk
        .enter(ProducerNode::Source, "source".into())
        .unwrap()
        .unwrap();
    assert!(walk.enter(ProducerNode::Work, "work".into()).is_err());
    walk.leave(&source, false);
    walk.leave(&work, false);
    assert!(walk
        .enter(ProducerNode::Work, "work".into())
        .unwrap()
        .is_some());
    assert!(walk
        .enter(ProducerNode::Source, "source".into())
        .unwrap()
        .is_some());
}

#[tokio::test]
async fn private_child_dispatch_preserves_original_budget_across_public_partitions() {
    for embedded in [true, false] {
        for finite in [false, true] {
            let storage = if embedded {
                StorageClient::new_embedded().await.unwrap()
            } else {
                let storage = StorageClient::new_memory().await.unwrap();
                elephant_migrations::apply_schema(&storage).await.unwrap();
                storage
            };
            let space = "private-budget";
            let base =
                |table| BaseFields::new(RecordId::new_v4(table), space.into(), "oracle".into());
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
            let jobs = ReprocessJobRepository::new(storage.clone());
            let job = jobs
                .create_with_work(ReprocessJob {
                    base: base(TableName::ReprocessJob),
                    name: "original".into(),
                    selection_criteria: SelectionCriteria {
                        source_types: vec!["manual".into()],
                        ..Default::default()
                    },
                    target_stages: vec![ReprocessTargetStage::ReMaintain],
                    status: ReprocessStatus::Pending,
                    progress: ReprocessProgress::default(),
                    priority: 1,
                    budget_limit_euros: finite.then_some(1.0),
                    max_sources_per_day: None,
                    started_at: None,
                    completed_at: None,
                    error: None,
                    reason: None,
                })
                .await
                .unwrap();
            let parent_id = jobs.ensure_work(&job).await.unwrap();
            let items = WorkItemRepository::new(storage.clone());
            let parent = items
                .claim_with_lease(
                    space,
                    &parent_id,
                    "oracle",
                    Utc::now() + chrono::Duration::minutes(5),
                )
                .await
                .unwrap()
                .unwrap();
            let fence =
                elephant_storage::work_attempt_repository::fence_from_claim(&parent).unwrap();
            let mut metrics = WorkRunMetrics::default();
            metrics.custom.insert(
                "attempt_fence".into(),
                serde_json::to_value(&fence).unwrap(),
            );
            let run = WorkRunRepository::new(storage.clone())
                .create(WorkRun {
                    base: base(TableName::WorkRun),
                    work_item: Link::new(parent_id),
                    attempt: parent.attempts,
                    started_at: Utc::now(),
                    completed_at: None,
                    success: false,
                    error: None,
                    metrics,
                    worker_id: fence.lease_token.clone(),
                    worker_version: "oracle".into(),
                    model_version: None,
                    prompt_id: None,
                    config_hash: None,
                })
                .await
                .unwrap();
            // A job created by actual queued canonical work inherits execution
            // authority; changing its own budget cannot erase that parent.
            let mut inherited = job.clone();
            inherited.base.id = RecordId::new_v4(TableName::ReprocessJob);
            inherited.name = "inherited original execution".into();
            inherited.budget_limit_euros = None;
            let planning =
                PlanningTruthMaintenanceStorage::new(storage.clone(), space, Some(fence.clone()))
                    .await
                    .unwrap();
            planning.stage(None, inherited.clone()).unwrap();
            planning
                .commit(
                    "test_inherited_reprocess",
                    &inherited.base.id.to_string(),
                    &serde_json::json!({"parent": parent.base.id}),
                    &(),
                )
                .await
                .unwrap();
            let inherited_id = jobs.ensure_work(&inherited).await.unwrap();
            let inherited_work = items.get(space, &inherited_id).await.unwrap().unwrap();
            assert!(matches!(
                WorkProducerRepository::new(storage.clone()).origin_for_job(&inherited).await.unwrap(),
                elephant_storage::work_producer_repository::WorkProducerOrigin::ReprocessJob {
                    execution_origin: elephant_storage::work_producer_repository::ReprocessExecutionOrigin::Inherited { .. }, ..
                }
            ));
            let mut group_base = base(TableName::ConflictGroup);
            group_base.security.policy_id = "private-group".into();
            group_base.security.level = 2;
            let group = ConflictGroupRepository::new(storage.clone())
                .create(ConflictGroup {
                    base: group_base,
                    slot_key: "private-budget-slot".into(),
                    slot_type: "rel_set".into(),
                    claim_ids: vec![],
                    resolved: false,
                    resolution_rationale: None,
                    detected_at: Utc::now(),
                    resolved_at: None,
                    adjudicated_claim_set_hash: None,
                    adjudicated_at: None,
                })
                .await
                .unwrap();
            let mut child_base = group.base.clone();
            child_base.id = RecordId::new_v4(TableName::WorkItem);
            let child = WorkProducerRepository::new(storage.clone())
                .enqueue_group_child(
                    &parent,
                    &run,
                    &group,
                    WorkItem {
                        base: child_base,
                        kind: "tm_resolve_slot".into(),
                        payload: serde_json::json!({"conflict_group_id":group.base.id.to_string()}),
                        status: WorkItemStatus::Runnable,
                        priority: 1,
                        idempotency_key: uuid::Uuid::new_v4().to_string(),
                        attempts: 0,
                        lease_expires_at: None,
                        leased_by: None,
                        run_after: None,
                        last_error: None,
                    },
                    false,
                )
                .await
                .unwrap();
            assert!(child.payload.get("_predecessor_work_item").is_none());
            assert_eq!(child.base.security, group.base.security);
            let mut current = job.clone();
            current.budget_limit_euros = None;
            jobs.update(space, &current.base.id.clone(), current)
                .await
                .unwrap();
            let inherited_result = guard_physical_dispatch(&storage, &inherited_work).await;
            if finite {
                assert!(
                    matches!(inherited_result, Err(PipelineError::BudgetExceeded(_))),
                    "{inherited_result:?}"
                );
            } else {
                inherited_result.unwrap();
            }
            let result = guard_physical_dispatch(&storage, &child).await;
            if finite {
                assert!(
                    matches!(result, Err(PipelineError::BudgetExceeded(_))),
                    "{result:?}"
                );
            } else {
                result.unwrap();
                let mut limited = jobs.get(space, &job.base.id).await.unwrap().unwrap();
                limited.budget_limit_euros = Some(2.0);
                jobs.update(space, &limited.base.id.clone(), limited)
                    .await
                    .unwrap();
                assert!(matches!(
                    guard_physical_dispatch(&storage, &child).await,
                    Err(PipelineError::BudgetExceeded(_))
                ));
            }
            assert!(matches!(
                guard_physical_dispatch(&storage, &inherited_work).await,
                Err(PipelineError::BudgetExceeded(_))
            ));
            // A genuinely new independent admission retains its own limit.
            // Identical selected content/intent does not inherit the old veto.
            let mut independent = inherited;
            independent.base.id = RecordId::new_v4(TableName::ReprocessJob);
            independent.name = "new independent root".into();
            let independent = jobs.create_with_work(independent).await.unwrap();
            let independent_id = jobs.ensure_work(&independent).await.unwrap();
            let independent_work = items.get(space, &independent_id).await.unwrap().unwrap();
            guard_physical_dispatch(&storage, &independent_work)
                .await
                .unwrap();

            // Historical run evidence remains mandatory even after the actual
            // parent claim completes; the inherited edge never adopts a retry.
            items
                .update_running_for_worker(
                    space,
                    &parent.base.id,
                    &fence.lease_token,
                    WorkItemStatus::Done,
                    None,
                )
                .await
                .unwrap()
                .unwrap();
            let mut ended = run.clone();
            ended.completed_at = Some(Utc::now());
            ended.success = true;
            let ended_id = ended.base.id.clone();
            WorkRunRepository::new(storage.clone())
                .update(space, &ended_id, ended.clone())
                .await
                .unwrap();
            assert!(matches!(
                guard_physical_dispatch(&storage, &inherited_work).await,
                Err(PipelineError::BudgetExceeded(_))
            ));
            ended.metrics.custom.remove("attempt_fence");
            WorkRunRepository::new(storage.clone())
                .update(space, &ended_id, ended)
                .await
                .unwrap();
            assert!(matches!(
                guard_physical_dispatch(&storage, &inherited_work).await,
                Err(PipelineError::StorageError(_))
            ));
        }
    }
}
