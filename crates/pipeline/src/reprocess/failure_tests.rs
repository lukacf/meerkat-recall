//! Real queue, canonical and archive contracts; no provider or model calls.
#![allow(clippy::unwrap_used, clippy::expect_used)]
#![allow(
    clippy::significant_drop_tightening,
    reason = "Regression fixtures keep original repository and record owners alive through after-state assertions."
)]

use super::*;
use chrono::{Duration, Utc};
use elephant_storage::canonical_commit::{CanonicalMaintenanceOrigin, CanonicalRecord};
use elephant_storage::work_attempt_repository::fence_from_claim;
use elephant_types::records::reprocess::{ReprocessProgress, ReprocessTargetStage};
use elephant_types::records::{BaseFields, ReprocessJob, SelectionCriteria, WorkRunMetrics};
use elephant_types::{Link, TableName};

const SPACE: &str = "terminal-reprocess-no-config";

async fn fixture(embedded: bool) -> (StorageClient, ReprocessJob, WorkItem, WorkRun) {
    let storage = if embedded {
        StorageClient::new_embedded().await.unwrap()
    } else {
        let storage = StorageClient::new_memory().await.unwrap();
        Box::pin(elephant_migrations::apply_schema(&storage))
            .await
            .unwrap();
        storage
    };
    // Canonical planning requires a registered space even for control-only
    // reconciliation. Empty processing bindings keep this fixture model-free.
    elephant_storage::SpaceConfigRepository::new(storage.clone())
        .create(
            serde_json::from_value(serde_json::json!({
                "id": RecordId::new_v4(TableName::SpaceConfig),
                "space_id": SPACE,
                "created_by": "fixture",
                "created_at": Utc::now(),
                "updated_at": Utc::now(),
                "deleted_at": null,
                "security": {"policy_id": "default", "level": 0, "labels": [], "handling": []},
                "provenance": {},
                "subjects": [],
                "subjects_state": "complete",
                "embedding_provider": "fake",
                "embedding_model": "fixture",
                "embedding_dims": 8,
                "processing": {
                    "generation": 1,
                    "index_generation": 0,
                    "policy_id": "fixture-no-model",
                    "connections": {},
                    "profiles": {},
                    "bindings": {},
                    "active_embedding_profile_id": null,
                    "pending_reindex": null
                }
            }))
            .unwrap(),
        )
        .await
        .unwrap();
    let mut base = BaseFields::new(
        RecordId::new_v4(TableName::ReprocessJob),
        SPACE.into(),
        "authorized-caller".into(),
    );
    base.security.level = 3;
    let job = ReprocessJobRepository::new(storage.clone())
        .create_with_work(ReprocessJob {
            base,
            name: "failed maintenance".into(),
            selection_criteria: SelectionCriteria::default(),
            target_stages: vec![ReprocessTargetStage::ReResolve],
            status: ReprocessStatus::Pending,
            progress: ReprocessProgress::default(),
            priority: 7,
            budget_limit_euros: None,
            max_sources_per_day: None,
            started_at: None,
            completed_at: None,
            error: None,
            reason: None,
        })
        .await
        .unwrap();
    let work_id = ReprocessJobRepository::new(storage.clone())
        .ensure_work(&job)
        .await
        .unwrap();
    let item = WorkItemRepository::new(storage.clone())
        .claim_with_lease(
            SPACE,
            &work_id,
            "original",
            Utc::now() + Duration::minutes(5),
        )
        .await
        .unwrap()
        .unwrap();
    let mut metrics = WorkRunMetrics::default();
    metrics.custom.insert(
        "attempt_fence".into(),
        item.payload[ATTEMPT_PAYLOAD_KEY].clone(),
    );
    let run = WorkRunRepository::new(storage.clone())
        .create(WorkRun {
            base: BaseFields::new(
                RecordId::new_v4(TableName::WorkRun),
                SPACE.into(),
                "worker".into(),
            ),
            work_item: Link::new(work_id),
            attempt: item.attempts,
            started_at: Utc::now(),
            completed_at: Some(Utc::now()),
            success: false,
            error: Some("private provider detail must not become public job text".into()),
            metrics,
            worker_id: "worker".into(),
            worker_version: "1".into(),
            model_version: None,
            prompt_id: None,
            config_hash: None,
        })
        .await
        .unwrap();
    (storage, job, item, run)
}

async fn fail(storage: &StorageClient, item: &WorkItem) -> WorkItem {
    let fence = fence_from_claim(item).unwrap();
    WorkItemRepository::new(storage.clone())
        .update_running_for_worker(
            SPACE,
            &item.base.id,
            &fence.lease_token,
            WorkItemStatus::Failed,
            Some("private failure".into()),
        )
        .await
        .unwrap()
        .unwrap()
}

async fn read_job(storage: &StorageClient, job: &ReprocessJob) -> ReprocessJob {
    ReprocessJobRepository::new(storage.clone())
        .get(SPACE, &job.base.id)
        .await
        .unwrap()
        .unwrap()
}

#[tokio::test]
async fn terminal_execution_updates_only_job_control_and_preserves_denominators() {
    for embedded in [true, false] {
        let (storage, job, item, run) = Box::pin(fixture(embedded)).await;
        let fence = fence_from_claim(&item).unwrap();
        assert!(
            !reconcile_failed_work(&storage, SPACE, &item.base.id, Some(&fence))
                .await
                .unwrap()
        );
        assert_eq!(
            read_job(&storage, &job).await.status,
            ReprocessStatus::Pending
        );
        let failed = fail(&storage, &item).await;
        let request = prepare_failure(&storage, SPACE, &item.base.id, Some(&fence))
            .await
            .unwrap()
            .unwrap();
        let context = request.maintenance.as_ref().unwrap();
        assert_eq!(context.origin, CanonicalMaintenanceOrigin::ControlOnly);
        assert!(
            context.attempt.is_none(),
            "closed evidence never becomes a live lease"
        );
        assert!(request.publication.is_none());
        assert!(request.finalization.is_none());
        assert!(request.plan.extension_effects.is_empty());
        assert_eq!(
            request.plan.effects.len(),
            2,
            "one job transition and its full audit"
        );
        assert!(request.plan.effects.iter().all(|effect| matches!(
            effect.after.as_ref(),
            Some(CanonicalRecord::ReprocessJob(_) | CanonicalRecord::AuditLog(_))
        )));
        CanonicalRepository::new(storage.clone())
            .commit(request)
            .await
            .unwrap();
        let actual = read_job(&storage, &job).await;
        assert_eq!(actual.status, ReprocessStatus::Failed);
        assert_eq!(actual.progress, job.progress);
        assert_eq!(actual.error.as_deref(), Some(FAILURE));
        assert_eq!(actual.started_at, Some(run.started_at));
        assert_eq!(actual.completed_at, Some(failed.base.updated_at));
        assert_eq!(actual.base.security, job.base.security);
        assert_eq!(actual.base.subjects, job.base.subjects);
        assert_eq!(actual.base.provenance, job.base.provenance);
        assert_eq!(
            WorkItemRepository::new(storage.clone())
                .get(SPACE, &item.base.id)
                .await
                .unwrap(),
            Some(failed)
        );
        assert!(!reconcile_failed_work(&storage, SPACE, &item.base.id, None)
            .await
            .unwrap());
    }
}

#[tokio::test]
async fn crash_gap_recovers_exact_archived_terminal_work_and_run() {
    for embedded in [true, false] {
        let (storage, job, item, run) = Box::pin(fixture(embedded)).await;
        let failed = fail(&storage, &item).await;
        assert_eq!(
            read_job(&storage, &job).await.status,
            ReprocessStatus::Pending
        );
        let archived = elephant_storage::work_retention::archive_terminal_work(
            &storage,
            Utc::now() + Duration::seconds(1),
            100,
        )
        .await
        .unwrap();
        assert_eq!(archived.items_archived, 1);
        assert_eq!(archived.runs_archived, 1);
        let jobs = ReprocessJobRepository::new(storage.clone());
        let recoverable = jobs.recoverable_after(256, None).await.unwrap();
        assert_eq!(recoverable.len(), 1);
        let original_id = jobs.ensure_work(&recoverable[0]).await.unwrap();
        assert_eq!(original_id, item.base.id);
        assert!(reconcile_failed_work(&storage, SPACE, &original_id, None)
            .await
            .unwrap());
        assert_eq!(
            read_job(&storage, &job).await.status,
            ReprocessStatus::Failed
        );
        assert_eq!(
            WorkItemRepository::new(storage.clone())
                .get(SPACE, &original_id)
                .await
                .unwrap(),
            Some(failed)
        );
        assert_eq!(
            serde_json::to_value(
                WorkRunRepository::new(storage.clone())
                    .get(SPACE, &run.base.id)
                    .await
                    .unwrap()
                    .unwrap()
            )
            .unwrap(),
            serde_json::to_value(&run).unwrap()
        );
        assert!(jobs.recoverable_after(256, None).await.unwrap().is_empty());
        assert!(!reconcile_failed_work(&storage, SPACE, &original_id, None)
            .await
            .unwrap());
    }
}

#[tokio::test]
async fn retry_and_stale_claim_cannot_close_public_job() {
    for embedded in [true, false] {
        let (storage, job, first, _) = Box::pin(fixture(embedded)).await;
        let old_fence = fence_from_claim(&first).unwrap();
        let items = WorkItemRepository::new(storage.clone());
        let mut retry = items
            .requeue_with_delay(
                SPACE,
                &first.base.id,
                &old_fence.lease_token,
                1,
                Some("retry".into()),
            )
            .await
            .unwrap()
            .unwrap();
        assert!(
            !reconcile_failed_work(&storage, SPACE, &first.base.id, Some(&old_fence))
                .await
                .unwrap()
        );
        retry.run_after = None;
        items.update(SPACE, &first.base.id, retry).await.unwrap();
        let current = items
            .claim_with_lease(
                SPACE,
                &first.base.id,
                "successor",
                Utc::now() + Duration::minutes(5),
            )
            .await
            .unwrap()
            .unwrap();
        assert!(items
            .update_running_for_worker(
                SPACE,
                &first.base.id,
                &old_fence.lease_token,
                WorkItemStatus::Failed,
                Some("stale".into())
            )
            .await
            .unwrap()
            .is_none());
        assert!(
            !reconcile_failed_work(&storage, SPACE, &first.base.id, Some(&old_fence))
                .await
                .unwrap()
        );
        fail(&storage, &current).await;
        assert!(
            reconcile_failed_work(&storage, SPACE, &first.base.id, Some(&old_fence))
                .await
                .is_err()
        );
        assert!(
            reconcile_failed_work(&storage, SPACE, &first.base.id, None)
                .await
                .is_err(),
            "previous attempt's failed run cannot prove current failure"
        );
        assert_eq!(
            read_job(&storage, &job).await.status,
            ReprocessStatus::Pending
        );
    }
}

#[tokio::test]
async fn changed_job_work_or_run_rejects_prepared_control_atomically() {
    for embedded in [true, false] {
        for changed in ["job", "work", "run"] {
            let (storage, job, item, mut run) = Box::pin(fixture(embedded)).await;
            let mut failed = fail(&storage, &item).await;
            let request = prepare_failure(&storage, SPACE, &item.base.id, None)
                .await
                .unwrap()
                .unwrap();
            match changed {
                "job" => {
                    let mut cancelled = job.clone();
                    cancelled.status = ReprocessStatus::Cancelled;
                    ReprocessJobRepository::new(storage.clone())
                        .update(SPACE, &job.base.id, cancelled)
                        .await
                        .unwrap();
                }
                "work" => {
                    failed.last_error = Some("changed after observation".into());
                    WorkItemRepository::new(storage.clone())
                        .update(SPACE, &item.base.id, failed)
                        .await
                        .unwrap();
                }
                _ => {
                    run.error = Some("changed after observation".into());
                    let run_id = run.base.id.clone();
                    WorkRunRepository::new(storage.clone())
                        .update(SPACE, &run_id, run)
                        .await
                        .unwrap();
                }
            }
            assert!(CanonicalRepository::new(storage.clone())
                .commit(request)
                .await
                .is_err());
            assert_eq!(
                read_job(&storage, &job).await.status,
                if changed == "job" {
                    ReprocessStatus::Cancelled
                } else {
                    ReprocessStatus::Pending
                }
            );
        }
    }
}

#[tokio::test]
async fn incomplete_or_ambiguous_execution_evidence_cannot_manufacture_failure() {
    for embedded in [true, false] {
        for changed in ["unfinished", "successful", "missing_fence", "duplicate"] {
            let (storage, job, item, mut run) = Box::pin(fixture(embedded)).await;
            fail(&storage, &item).await;
            let runs = WorkRunRepository::new(storage.clone());
            match changed {
                "unfinished" => run.completed_at = None,
                "successful" => run.success = true,
                "missing_fence" => {
                    run.metrics.custom.remove("attempt_fence");
                }
                _ => {
                    let mut duplicate = run.clone();
                    duplicate.base.id = RecordId::new_v4(TableName::WorkRun);
                    runs.create(duplicate).await.unwrap();
                }
            }
            let run_id = run.base.id.clone();
            runs.update(SPACE, &run_id, run).await.unwrap();
            assert!(reconcile_failed_work(&storage, SPACE, &item.base.id, None)
                .await
                .is_err());
            assert_eq!(
                read_job(&storage, &job).await.status,
                ReprocessStatus::Pending
            );
        }
    }
}

#[tokio::test]
async fn paused_cancelled_completed_and_failed_jobs_keep_existing_outcomes() {
    for embedded in [true, false] {
        let (storage, job, item, _) = Box::pin(fixture(embedded)).await;
        fail(&storage, &item).await;
        for status in [
            ReprocessStatus::Paused,
            ReprocessStatus::Cancelled,
            ReprocessStatus::Completed,
            ReprocessStatus::Failed,
        ] {
            let mut retained = job.clone();
            retained.status = status;
            retained.error = Some("existing control decision".into());
            ReprocessJobRepository::new(storage.clone())
                .update(SPACE, &job.base.id, retained.clone())
                .await
                .unwrap();
            assert!(!reconcile_failed_work(&storage, SPACE, &item.base.id, None)
                .await
                .unwrap());
            assert_eq!(
                serde_json::to_value(read_job(&storage, &job).await).unwrap(),
                serde_json::to_value(&retained).unwrap()
            );
        }
    }
}
