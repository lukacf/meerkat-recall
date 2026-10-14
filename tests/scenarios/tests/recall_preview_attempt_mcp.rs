//! Public work-item adapters must preserve the caller's original attempt.
//! Actual MCP dispatcher/repositories with fixed non-bypass ABAC on both backends.
//! Signed transport and in-admission envelope races have separate oracles.

#[path = "../../recall_preview/support.rs"]
mod support;

use elephant_mcp::handlers::work_items::{ClaimWorkItemHandler, CompleteWorkItemHandler};
use elephant_mcp::{McpError, ToolContext, ToolDispatcher};
use elephant_policy::{Clearance, Principal};
use elephant_storage::work_attempt_repository::fence_from_claim;
use elephant_storage::{Repository, StorageClient, WorkItemRepository, WorkRunRepository};
use elephant_types::public_error::ErrorCode;
use elephant_types::records::base::{BaseFields, SubjectsState};
use elephant_types::records::work::{WorkItem, WorkItemStatus};
use elephant_types::{RecordId, TableName};
use serde_json::{json, Value};

const SPACE: &str = "qualification-public-attempt";
const OTHER_SPACE: &str = "qualification-other-attempt";
const WORKER: &str = "same-worker-label";
const CANARY: &str = "private-work-payload-must-not-enter-refusal";

fn principal(space: &str, level: i32, scopes: &[&str]) -> Principal {
    Principal {
        space_id: space.into(),
        scopes: scopes.iter().map(|scope| (*scope).to_owned()).collect(),
        clearances: vec![Clearance {
            policy_id: "default".into(),
            max_level: level,
            labels: if level == 0 {
                Default::default()
            } else {
                ["private-work".into()].into()
            },
            handling_allow: Default::default(),
        }],
        subject_allowlist: None,
        purpose: Default::default(),
    }
}

fn dispatcher(storage: &StorageClient, principal: Principal) -> ToolDispatcher {
    let context = ToolContext::new(storage.clone(), principal);
    assert!(!context.authz.is_skip_auth());
    let mut dispatcher = ToolDispatcher::new(context);
    dispatcher.register(ClaimWorkItemHandler::new(storage.clone()));
    dispatcher.register(CompleteWorkItemHandler::new(storage.clone()));
    dispatcher
}

fn work(space: &str, name: &str) -> WorkItem {
    let mut base = BaseFields::new(
        RecordId::new_v4(TableName::WorkItem),
        space.into(),
        "D".into(),
    );
    base.subjects_state = SubjectsState::Complete;
    WorkItem {
        base,
        kind: "qualification_public_attempt".into(),
        payload: json!({"private_canary":CANARY}),
        status: WorkItemStatus::Runnable,
        priority: 1,
        idempotency_key: format!("{space}/{name}"),
        attempts: 0,
        lease_expires_at: None,
        leased_by: None,
        run_after: None,
        last_error: None,
    }
}

async fn stored(repository: &WorkItemRepository, item: &WorkItem) -> anyhow::Result<WorkItem> {
    repository
        .get(&item.base.space_id, &item.base.id)
        .await?
        .ok_or_else(|| anyhow::anyhow!("fixture work disappeared"))
}

async fn reclassify_fixture(
    storage: &StorageClient,
    original: &WorkItem,
) -> anyhow::Result<WorkItem> {
    // Inject only a concurrent envelope change through the actual backend.
    // Generic WorkItem updates correctly prohibit rewriting running attempts;
    // this fixture does not claim a public reclassification API or edit a lease.
    if let Some(store) = storage.embedded_store() {
        let mut changed = original.clone();
        changed.base.security.level = 5;
        changed.base.security.labels = vec!["private-work".into()];
        store.replace(changed).await?;
    } else {
        storage
            .query_bind_checked(
                "UPDATE $id SET security.level=5,security.labels=['private-work'];",
                elephant_storage::bindings! {
                    "id" => elephant_storage::surreal::thing_from_record_id(&original.base.id),
                },
            )
            .await?;
    }
    let changed = stored(&WorkItemRepository::new(storage.clone()), original).await?;
    anyhow::ensure!(changed.base.security.level == 5);
    anyhow::ensure!(changed.leased_by == original.leased_by && changed.payload == original.payload);
    anyhow::ensure!(changed.status == original.status && changed.attempts == original.attempts);
    Ok(changed)
}

async fn claim(dispatcher: &ToolDispatcher, item: &WorkItem) -> anyhow::Result<String> {
    let result = dispatcher
        .execute(
            "claim_work_item",
            json!({
                "id":item.base.id,"worker_id":WORKER,"lease_duration_secs":120
            }),
        )
        .await?;
    let result = support::mcp_success_data(result)?;
    anyhow::ensure!(result["claimed"] == true && result["id"] == item.base.id.to_string());
    let token = result["lease_token"]
        .as_str()
        .filter(|token| !token.is_empty() && *token != WORKER)
        .ok_or_else(|| anyhow::anyhow!("claim did not return an opaque committed lease token"))?;
    Ok(token.to_owned())
}

async fn complete(
    dispatcher: &ToolDispatcher,
    item: &WorkItem,
    token: &str,
) -> Result<Value, McpError> {
    dispatcher
        .execute(
            "complete_work_item",
            json!({
                "id":item.base.id,"lease_token":token,"success":true
            }),
        )
        .await
}

fn refused(result: Result<Value, McpError>, codes: &[ErrorCode]) -> anyhow::Result<()> {
    match result {
        Err(McpError::Public(error)) => {
            anyhow::ensure!(
                codes.contains(&error.error_code),
                "wrong refusal: {error:?}"
            );
            anyhow::ensure!(
                !error.retryable,
                "unchanged unauthorized/stale completion cannot retry"
            );
            anyhow::ensure!(
                !error.message.contains(CANARY),
                "private work payload leaked"
            );
            Ok(())
        }
        other => anyhow::bail!("expected an explicit public refusal, got {other:?}"),
    }
}

#[tokio::test]
async fn mcp_completion_cannot_adopt_successor_or_other_work_lease() -> anyhow::Result<()> {
    for backend in support::BACKENDS {
        let storage = support::storage(backend).await?;
        let repository = WorkItemRepository::new(storage.clone());
        let dispatcher = dispatcher(
            &storage,
            principal(SPACE, 0, &["read:work_item", "write:work_item"]),
        );
        let item = repository
            .create(work(SPACE, "same-worker-reclaim"))
            .await?;
        for duration in [0, u64::MAX, 100_000_000_000_000] {
            anyhow::ensure!(
                matches!(
                    dispatcher
                        .execute(
                            "claim_work_item",
                            json!({
                                "id":item.base.id,"worker_id":WORKER,"lease_duration_secs":duration
                            })
                        )
                        .await,
                    Err(McpError::InvalidParameters(_))
                ),
                "invalid lease duration was not refused"
            );
            anyhow::ensure!(stored(&repository, &item).await? == item);
        }
        let original_token = claim(&dispatcher, &item).await?;
        let original = stored(&repository, &item).await?;
        let original_fence = fence_from_claim(&original)?;
        anyhow::ensure!(
            original_fence.generation == 1 && original_fence.lease_token == original_token
        );
        anyhow::ensure!(original.leased_by.as_deref() == Some(original_token.as_str()));

        for argument in [
            json!({}),
            json!({"lease_token":""}),
            json!({"lease_token":" \t "}),
            json!({"lease_token":null}),
        ] {
            let mut input = argument;
            input["id"] = json!(item.base.id);
            input["success"] = json!(true);
            anyhow::ensure!(
                matches!(
                    dispatcher.execute("complete_work_item", input).await,
                    Err(McpError::InvalidParameters(_))
                ),
                "missing/empty original token was accepted or misclassified"
            );
            anyhow::ensure!(stored(&repository, &item).await? == original);
        }
        refused(
            complete(&dispatcher, &item, WORKER).await,
            &[ErrorCode::ConflictGeneration],
        )?;
        refused(
            complete(&dispatcher, &item, &format!(" {original_token} ")).await,
            &[ErrorCode::ConflictGeneration],
        )?;
        anyhow::ensure!(stored(&repository, &item).await? == original);

        // Use the actual original-token transition; never fabricate a successor
        // fence or set a past lease directly in a stored payload.
        let requeued = repository
            .requeue_with_delay(
                SPACE,
                &item.base.id,
                &original_token,
                0,
                Some("controlled retry".into()),
            )
            .await?
            .ok_or_else(|| anyhow::anyhow!("original owner could not requeue"))?;
        anyhow::ensure!(requeued.status == WorkItemStatus::Runnable);
        let successor_token = claim(&dispatcher, &item).await?;
        let successor = stored(&repository, &item).await?;
        let successor_fence = fence_from_claim(&successor)?;
        anyhow::ensure!(successor_token != original_token);
        anyhow::ensure!(successor_fence.generation == original_fence.generation + 1);
        anyhow::ensure!(successor_fence.lease_token == successor_token && successor.attempts == 2);

        for success in [true, false] {
            refused(
                dispatcher
                    .execute(
                        "complete_work_item",
                        json!({
                            "id":item.base.id,"lease_token":original_token,
                            "success":success,"error":"old attempt result"
                        }),
                    )
                    .await,
                &[ErrorCode::ConflictGeneration],
            )?;
            anyhow::ensure!(
                stored(&repository, &item).await? == successor,
                "old caller changed successor status/lease/payload"
            );
        }

        let other = repository
            .create(work(SPACE, "different-original-work"))
            .await?;
        let other_token = claim(&dispatcher, &other).await?;
        let other_before = stored(&repository, &other).await?;
        refused(
            complete(&dispatcher, &item, &other_token).await,
            &[ErrorCode::ConflictGeneration],
        )?;
        anyhow::ensure!(stored(&repository, &item).await? == successor);
        anyhow::ensure!(stored(&repository, &other).await? == other_before);

        let result =
            support::mcp_success_data(complete(&dispatcher, &item, &successor_token).await?)?;
        anyhow::ensure!(result["completed"] == true);
        let completed = stored(&repository, &item).await?;
        anyhow::ensure!(completed.status == WorkItemStatus::Done && completed.attempts == 2);
        anyhow::ensure!(completed.payload["private_canary"] == CANARY);
        anyhow::ensure!(stored(&repository, &other).await? == other_before);
        anyhow::ensure!(
            WorkRunRepository::new(storage)
                .find_by_work_item(&item.base.id)
                .await?
                .is_empty(),
            "queue completion fabricated pipeline execution"
        );
    }
    Ok(())
}

#[tokio::test]
async fn mcp_claim_and_complete_require_current_record_scope_space_and_clearance(
) -> anyhow::Result<()> {
    for backend in support::BACKENDS {
        let storage = support::storage(backend).await?;
        let repository = WorkItemRepository::new(storage.clone());
        let low = dispatcher(
            &storage,
            principal(SPACE, 0, &["read:work_item", "write:work_item"]),
        );
        let cleared = dispatcher(
            &storage,
            principal(SPACE, 5, &["read:work_item", "write:work_item"]),
        );
        let other_space = dispatcher(
            &storage,
            principal(OTHER_SPACE, 0, &["read:work_item", "write:work_item"]),
        );
        let mut private = work(SPACE, "classified");
        private.base.security.level = 5;
        private.base.security.labels = vec!["private-work".into()];
        let private = repository.create(private).await?;
        refused(
            low.execute(
                "claim_work_item",
                json!({"id":private.base.id,"worker_id":WORKER}),
            )
            .await,
            &[ErrorCode::ForbiddenSpace],
        )?;
        anyhow::ensure!(stored(&repository, &private).await? == private);
        let private_token = claim(&cleared, &private).await?;
        let private_running = stored(&repository, &private).await?;
        refused(
            complete(&low, &private, &private_token).await,
            &[ErrorCode::ForbiddenSpace],
        )?;
        anyhow::ensure!(stored(&repository, &private).await? == private_running);
        refused(
            complete(&other_space, &private, &private_token).await,
            &[ErrorCode::ConflictGeneration],
        )?;
        anyhow::ensure!(stored(&repository, &private).await? == private_running);
        support::mcp_success_data(complete(&cleared, &private, &private_token).await?)?;
        anyhow::ensure!(stored(&repository, &private).await?.status == WorkItemStatus::Done);

        let item = repository
            .create(work(SPACE, "scope-and-reclassification"))
            .await?;
        for scopes in [&["read:work_item"][..], &["write:work_item"][..]] {
            let restricted = dispatcher(&storage, principal(SPACE, 0, scopes));
            refused(
                restricted
                    .execute(
                        "claim_work_item",
                        json!({"id":item.base.id,"worker_id":WORKER}),
                    )
                    .await,
                &[ErrorCode::ForbiddenSpace],
            )?;
            anyhow::ensure!(stored(&repository, &item).await? == item);
        }
        let token = claim(&low, &item).await?;
        let claimed = stored(&repository, &item).await?;
        let classified = reclassify_fixture(&storage, &claimed).await?;
        refused(
            complete(&low, &item, &token).await,
            &[ErrorCode::ForbiddenSpace],
        )?;
        anyhow::ensure!(
            stored(&repository, &item).await? == classified,
            "lease possession replaced current record authorization"
        );
        support::mcp_success_data(complete(&cleared, &item, &token).await?)?;
        anyhow::ensure!(
            stored(&repository, &item).await?.base.security == classified.base.security
        );

        let other = repository
            .create(work(OTHER_SPACE, "other-space-positive"))
            .await?;
        refused(
            low.execute(
                "claim_work_item",
                json!({"id":other.base.id,"worker_id":WORKER}),
            )
            .await,
            &[ErrorCode::ConflictGeneration],
        )?;
        anyhow::ensure!(stored(&repository, &other).await? == other);
        let other_token = claim(&other_space, &other).await?;
        support::mcp_success_data(complete(&other_space, &other, &other_token).await?)?;
        anyhow::ensure!(stored(&repository, &other).await?.status == WorkItemStatus::Done);
    }
    Ok(())
}
