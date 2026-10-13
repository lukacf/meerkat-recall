//! Quiet-space recovery exercises the real claim selector, not manual recovery.

#[path = "../../recall_preview/support.rs"]
mod support;

use chrono::{Duration, Utc};
use elephant_pipeline::scheduler::{work_queue::WorkQueueConfig, WorkQueue};
use elephant_storage::work_attempt_repository::fence_from_claim;
use elephant_storage::{Repository, WorkItemRepository};
use elephant_types::records::base::BaseFields;
use elephant_types::records::work::{WorkItem, WorkItemStatus};
use elephant_types::{RecordId, TableName};

#[tokio::test]
async fn quiet_named_spaces_recover_without_default_or_unrelated_runnable_work(
) -> anyhow::Result<()> {
    for backend in support::BACKENDS {
        for selector in ["ordinary", "batch", "preferred"] {
            let client = support::storage(backend).await?;
            let repo = WorkItemRepository::new(client.clone());
            let mut ids = Vec::new();
            let mut latest_expiry = Utc::now();
            for space in ["quiet-household", "quiet-business"] {
                let item = repo
                    .create(WorkItem {
                        base: BaseFields::new(
                            RecordId::new_v4(TableName::WorkItem),
                            space.into(),
                            "qualification".into(),
                        ),
                        kind: "triage_source".into(),
                        payload: serde_json::json!({}),
                        status: WorkItemStatus::Runnable,
                        priority: 0,
                        idempotency_key: format!("quiet/{space}"),
                        attempts: 0,
                        lease_expires_at: None,
                        leased_by: None,
                        run_after: None,
                        last_error: None,
                    })
                    .await?;
                let expiry = Utc::now() + Duration::seconds(1);
                let abandoned = repo
                    .claim_with_lease(space, &item.base.id, "dead-worker", expiry)
                    .await?;
                anyhow::ensure!(
                    abandoned.is_some(),
                    "fixture failed to acquire original lease"
                );
                let abandoned =
                    abandoned.ok_or_else(|| anyhow::anyhow!("original claim missing"))?;
                anyhow::ensure!(fence_from_claim(&abandoned)?.generation == 1);
                latest_expiry = latest_expiry.max(expiry);
                anyhow::ensure!(repo
                    .find_by_status(space, WorkItemStatus::Runnable)
                    .await?
                    .is_empty());
                ids.push(item.base.id);
            }
            // Allow the real lease to expire. A fabricated past-expiry claim is
            // correctly refused by the production claim authority.
            if let Ok(remaining) = latest_expiry.signed_duration_since(Utc::now()).to_std() {
                tokio::time::sleep(remaining + std::time::Duration::from_millis(25)).await;
            }
            let queue = WorkQueue::new(
                client,
                WorkQueueConfig {
                    worker_id: "replacement-worker".into(),
                    ..WorkQueueConfig::default()
                },
            );
            let mut recovered = Vec::new();
            for _ in 0..2 {
                let item = match selector {
                    "ordinary" => queue.claim().await?,
                    "batch" => queue
                        .claim_batch_matching(1, |_| true)
                        .await?
                        .into_iter()
                        .next(),
                    "preferred" => queue.claim_prefer_kind("triage_source", |_| true).await?,
                    _ => anyhow::bail!("unknown queue fixture selector"),
                }
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "{backend:?}/{selector}: stranded named-space work was not discovered"
                    )
                })?;
                anyhow::ensure!(item.status == WorkItemStatus::Running);
                let fence = fence_from_claim(&item)?;
                anyhow::ensure!(fence.generation == 2);
                anyhow::ensure!(fence.lease_token.starts_with("replacement-worker:"));
                anyhow::ensure!(item.leased_by.as_deref() == Some(fence.lease_token.as_str()));
                anyhow::ensure!(item.attempts == 2);
                anyhow::ensure!(item
                    .lease_expires_at
                    .is_some_and(|deadline| deadline > Utc::now()));
                recovered.push(item.base.id);
            }
            anyhow::ensure!(
                ids.iter().all(|id| recovered.contains(id)),
                "{backend:?}: one quiet space starved"
            );
            anyhow::ensure!(
                queue.claim().await?.is_none(),
                "duplicate claim of running recovery work"
            );
        }
    }
    Ok(())
}
