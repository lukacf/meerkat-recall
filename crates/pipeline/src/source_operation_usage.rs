//! Read-only projection of original-operation physical receipts. This is not a
//! pricing ledger, budget admission authority or source completion authority.
use super::{AttemptUsage, UsageBasis};
use crate::{PipelineError, PipelineResult};
use elephant_storage::provider_usage_repository::ProviderUsageRepository;
use elephant_storage::source_publication::SourcePublicationRepository;
use elephant_storage::{Repository, StorageClient, WorkItemRepository, WorkRunRepository};
use elephant_types::provider_binding::SpaceConfigToken;
use elephant_types::publication::{AttemptFence, OperationState};
use elephant_types::records::work::WorkRun;
use elephant_types::RecordId;
use std::collections::{BTreeMap, BTreeSet};

/// Derived accounting for an original accepted source operation across all its
/// attempts and configuration plans, including failed/superseded plans.
#[derive(Debug, Clone, PartialEq)]
pub struct SourceOperationUsage {
    /// Original operation identity; never replaced with the current head.
    pub operation_id: String,
    /// Actual monetary total, unknown when no concrete billing evidence exists.
    /// Current receipts support known zero only for closed no-dispatch work.
    pub monetary_euros: Option<f64>,
    /// Complete input total, or unknown if any contributing input is unknown.
    pub input_tokens: Option<u64>,
    /// Complete output total, or unknown if any contributing output is unknown.
    pub output_tokens: Option<u64>,
    /// The owner reports Completed and all observed execution/usage is closed.
    /// This does not imply that monetary pricing is known.
    pub complete: bool,
    /// Number of distinct durable physical dispatch receipts.
    pub physical_calls: u64,
}

struct Aggregate {
    owner_completed: bool,
    seen_runs: usize,
    closed: bool,
    input: Option<u64>,
    output: Option<u64>,
    calls: u64,
}
impl Aggregate {
    const fn new(owner_completed: bool) -> Self {
        Self {
            owner_completed,
            seen_runs: 0,
            closed: true,
            input: Some(0),
            output: Some(0),
            calls: 0,
        }
    }
    fn unknown(&mut self) {
        self.closed = false;
        self.input = None;
        self.output = None;
    }
    fn finish(self, operation_id: String) -> SourceOperationUsage {
        let complete = self.owner_completed && self.seen_runs > 0 && self.closed;
        SourceOperationUsage {
            operation_id,
            monetary_euros: (complete
                && self.calls == 0
                && self.input == Some(0)
                && self.output == Some(0))
            .then_some(0.0),
            input_tokens: (self.seen_runs > 0).then_some(self.input).flatten(),
            output_tokens: (self.seen_runs > 0).then_some(self.output).flatten(),
            complete,
            physical_calls: self.calls,
        }
    }
}

fn invalid(message: &str) -> PipelineError {
    PipelineError::InvalidState(message.into())
}
fn add(total: Option<u64>, next: Option<u64>) -> PipelineResult<Option<u64>> {
    match (total, next) {
        (Some(total), Some(next)) => total
            .checked_add(next)
            .map(Some)
            .ok_or_else(|| invalid("Source operation token usage overflow")),
        _ => Ok(None),
    }
}

/// Read original source-operation usage from paged hot/archive work history and
/// durable physical-call receipts. Duplicate requested operation IDs are rejected.
/// The caller is responsible for authorizing access to the requested operations.
/// This projection does not reserve or enforce a currency budget.
///
/// # Errors
/// Rejects batches above 256, absent/foreign operations, malformed original
/// lineage, receipt identity conflicts, pagination nonprogress and overflow.
pub async fn source_operation_usage(
    storage: &StorageClient,
    space: &str,
    operation_ids: &[String],
) -> PipelineResult<Vec<SourceOperationUsage>> {
    if space.is_empty()
        || operation_ids.len() > 256
        || operation_ids.iter().any(String::is_empty)
        || operation_ids.iter().collect::<BTreeSet<_>>().len() != operation_ids.len()
    {
        return Err(PipelineError::InvalidInput(
            "Invalid operation usage batch".into(),
        ));
    }
    storage
        .with_activity(project(storage, space, operation_ids))
        .await?
}

async fn project(
    storage: &StorageClient,
    space: &str,
    ids: &[String],
) -> PipelineResult<Vec<SourceOperationUsage>> {
    if ids.is_empty() {
        return Ok(Vec::new());
    }
    let owner = SourcePublicationRepository::new(storage.clone());
    let mut operations = BTreeMap::new();
    let mut totals = BTreeMap::new();
    for id in ids {
        let receipt = owner.operation(space, id).await?.ok_or_else(|| {
            PipelineError::NotFound("Source operation usage owner missing".into())
        })?;
        if receipt.space_id != space || &receipt.operation_id != id {
            return Err(invalid("Source operation receipt identity mismatch"));
        }
        totals.insert(
            id.clone(),
            Aggregate::new(receipt.state == OperationState::Completed),
        );
        operations.insert(id.clone(), receipt);
    }
    let items = WorkItemRepository::new(storage.clone());
    let runs = WorkRunRepository::new(storage.clone());
    // Keep only requested lineage, while reading all history in bounded pages.
    let mut lineage: BTreeMap<RecordId, (String, Option<SpaceConfigToken>, bool)> = BTreeMap::new();
    let mut cursor = None;
    loop {
        let page = items.list(space, 256, cursor.as_ref()).await?;
        if page.is_empty() {
            break;
        }
        for item in &page {
            let Some(publication) = crate::source_queue::publication_from_work(item)? else {
                continue;
            };
            let Some(receipt) = operations.get(&publication.operation_id) else {
                continue;
            };
            if item.base.space_id != space
                || receipt.source_id != publication.source_id
                || receipt.source_generation != publication.generation
                || receipt.desired_revision.as_deref() != Some(publication.revision_id.as_str())
            {
                return Err(invalid(
                    "Work publication differs from original operation receipt",
                ));
            }
            let configuration = crate::source_queue::configuration_from_work(item)?;
            if lineage
                .insert(
                    item.base.id.clone(),
                    (publication.operation_id, configuration, false),
                )
                .is_some()
            {
                return Err(invalid("Duplicate source work history identity"));
            }
        }
        let next = page.last().map(|item| item.base.id.clone());
        if next == cursor {
            return Err(invalid("Source usage work cursor did not advance"));
        }
        cursor = next;
        if page.len() < 256 {
            break;
        }
    }
    let receipts = ProviderUsageRepository::new(storage.clone());
    let mut seen_calls = BTreeSet::new();
    let mut run_cursor = None;
    loop {
        let page = runs.list(space, 256, run_cursor.as_ref()).await?;
        if page.is_empty() {
            break;
        }
        for run in &page {
            let Some((operation_id, configuration, seen_run)) = lineage.get_mut(run.work_item.id())
            else {
                continue;
            };
            *seen_run = true;
            let total = totals
                .get_mut(operation_id)
                .ok_or_else(|| invalid("Usage accumulator missing"))?;
            let calls = receipts.for_scope(space, &run.base.id.to_string()).await?;
            add_run(total, run, configuration.as_ref(), &calls, &mut seen_calls)?;
        }
        let next = page.last().map(|run| run.base.id.clone());
        if next == run_cursor {
            return Err(invalid("Source usage run cursor did not advance"));
        }
        run_cursor = next;
        if page.len() < 256 {
            break;
        }
    }
    for (operation_id, _, seen_run) in lineage.values() {
        if !seen_run {
            totals
                .get_mut(operation_id)
                .ok_or_else(|| invalid("Usage accumulator missing"))?
                .unknown();
        }
    }
    ids.iter()
        .map(|id| {
            totals
                .remove(id)
                .map(|total| total.finish(id.clone()))
                .ok_or_else(|| invalid("Usage accumulator missing"))
        })
        .collect()
}

fn add_run(
    total: &mut Aggregate,
    run: &WorkRun,
    configuration: Option<&SpaceConfigToken>,
    receipts: &[elephant_storage::provider_usage_repository::ProviderCallReceipt],
    seen_calls: &mut BTreeSet<String>,
) -> PipelineResult<()> {
    total.seen_runs += 1;
    if run.completed_at.is_none() {
        total.closed = false;
    }
    let usage = AttemptUsage::from_metrics(&run.metrics)
        .map_err(|error| PipelineError::ValidationError(error.to_string()))?;
    let attempt = run
        .metrics
        .custom
        .get("attempt_fence")
        .cloned()
        .map(serde_json::from_value::<AttemptFence>)
        .transpose()
        .map_err(|error| PipelineError::ValidationError(error.to_string()))?;
    if let Some(attempt) = &attempt {
        if attempt.space_id != run.base.space_id
            || &attempt.work_item_id != run.work_item.id()
            || attempt.generation == 0
            || attempt.lease_token.is_empty()
        {
            return Err(invalid("WorkRun original attempt identity mismatch"));
        }
    }
    if receipts.is_empty() {
        // Historical/missing metrics, estimates and lost known receipts cannot
        // be relabeled as zero. Reservations alone are not consumption.
        if attempt.is_none()
            || usage.as_ref().is_none_or(|usage| {
                usage.basis != UsageBasis::NotStarted
                    || usage.measured_tokens.is_some_and(|tokens| tokens != 0)
                    || usage.estimated_tokens.is_some()
            })
        {
            total.unknown();
        }
        return Ok(());
    }
    for receipt in receipts {
        if receipt.space_id != run.base.space_id
            || receipt.scope_id != run.base.id.to_string()
            || attempt.is_none()
            || receipt.attempt.as_ref() != attempt.as_ref()
            || configuration
                .is_some_and(|configuration| &receipt.provenance.config != configuration)
        {
            return Err(invalid(
                "Physical receipt differs from original WorkRun authority",
            ));
        }
        if !seen_calls.insert(receipt.call_id.clone()) {
            return Err(invalid("Physical call appears in multiple WorkRuns"));
        }
        total.calls = total
            .calls
            .checked_add(1)
            .ok_or_else(|| invalid("Physical dispatch count overflow"))?;
        total.input = add(
            total.input,
            receipt
                .usage_complete
                .then_some(receipt.input_tokens)
                .flatten(),
        )?;
        total.output = add(
            total.output,
            receipt
                .usage_complete
                .then_some(receipt.output_tokens)
                .flatten(),
        )?;
        if !receipt.usage_complete
            || receipt.input_tokens.is_none()
            || receipt.output_tokens.is_none()
        {
            total.closed = false;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used)]
    use super::*;
    use elephant_embeddings::governed::ProviderDispatchObserver;
    use elephant_storage::source_publication::AcceptSourceRevision;
    use elephant_storage::SpaceConfigRepository;
    use elephant_types::provider_binding::{ModelBindingProvenance, ModelOperation};
    use elephant_types::publication::{PublicationFence, SourcePrecondition};
    use elephant_types::records::base::BaseFields;
    use elephant_types::records::source::{Source, SourceType};
    use elephant_types::records::work::{WorkItemStatus, WorkRunMetrics};
    use elephant_types::{Link, TableName};

    async fn contract(storage: StorageClient) {
        let space = "operation-usage-quiet";
        let config = serde_json::from_value(serde_json::json!({
            "id":RecordId::new_v4(TableName::SpaceConfig),"space_id":space,"created_by":"fixture",
            "created_at":chrono::Utc::now(),"updated_at":chrono::Utc::now(),"deleted_at":null,
            "security":{"policy_id":"default","level":0,"labels":[],"handling":[]},"provenance":{},"subjects":[],"subjects_state":"complete",
            "embedding_provider":"fake","embedding_model":"fixture","embedding_dims":8,
            "processing":{"generation":1,"index_generation":0,"policy_id":"no-network-fixture","connections":{},"profiles":{},"bindings":{},"active_embedding_profile_id":null,"pending_reindex":null}
        })).expect("space config");
        let configs = SpaceConfigRepository::new(storage.clone());
        configs.create(config).await.expect("explicit space");
        let configuration = configs.resolve_space(space).await.expect("resolve").token;
        let source = Source::new(
            BaseFields::new(
                RecordId::new_v4(TableName::Source),
                space.into(),
                "fixture".into(),
            ),
            SourceType::Manual,
            "fixture".into(),
        );
        let receipt = SourcePublicationRepository::new(storage.clone())
            .accept(AcceptSourceRevision {
                source: source.clone(),
                revision_id: "immutable-v1".into(),
                expected: SourcePrecondition::Absent,
                idempotency_key: "usage-accept".into(),
                request_fingerprint: "usage-input".into(),
                reactivate: false,
                recreate_after_erasure: false,
                authorized: None,
                authorized_document: None,
            })
            .await
            .expect("real accepted operation");
        let publication = PublicationFence {
            source_id: receipt.source_id,
            generation: receipt.source_generation,
            revision_id: "immutable-v1".into(),
            operation_id: receipt.operation_id.clone(),
        };
        let ids = vec![receipt.operation_id];
        let item_id = crate::source_queue::enqueue_source_plan(
            &storage,
            &source,
            &publication,
            &configuration,
            "fixture",
        )
        .await
        .expect("durable original queue");
        let before = source_operation_usage(&storage, space, &ids)
            .await
            .expect("no execution");
        assert_eq!(before[0].monetary_euros, None);
        assert_eq!(before[0].input_tokens, None);
        assert!(!before[0].complete);
        let items = WorkItemRepository::new(storage.clone());
        let item = items
            .claim_with_lease(
                space,
                &item_id,
                "original",
                chrono::Utc::now() + chrono::Duration::minutes(5),
            )
            .await
            .expect("claim")
            .expect("owned");
        let attempt = elephant_storage::work_attempt_repository::fence_from_claim(&item)
            .expect("original claim");
        let run_id = RecordId::new_v4(TableName::WorkRun);
        let mut metrics = WorkRunMetrics::default();
        AttemptUsage::reserved(1000, 5.0)
            .write_metrics(&mut metrics)
            .expect("reservation");
        metrics.custom.insert(
            "attempt_fence".into(),
            serde_json::to_value(&attempt).expect("fence"),
        );
        let runs = WorkRunRepository::new(storage.clone());
        let mut run = runs
            .create(WorkRun {
                base: BaseFields::new(run_id.clone(), space.into(), "fixture".into()),
                work_item: Link::new(item_id.clone()),
                attempt: 1,
                started_at: chrono::Utc::now(),
                completed_at: None,
                success: false,
                error: None,
                metrics,
                worker_id: "fixture".into(),
                worker_version: "fixture".into(),
                model_version: None,
                prompt_id: None,
                config_hash: None,
            })
            .await
            .expect("real started run");
        let observer = super::super::DurableUsageObserver::for_attempt(
            storage.clone(),
            &run_id,
            attempt.clone(),
        );
        let binding = ModelBindingProvenance {
            config: configuration,
            operation: ModelOperation::DocumentEmbedding,
            binding_id: "fixture".into(),
            profile_id: "fixture".into(),
        };
        let first = uuid::Uuid::new_v4().to_string();
        observer
            .before_dispatch(&first, &binding)
            .await
            .expect("durable physical receipt without network");
        observer
            .observe_usage(&first, &binding, Some(8), Some(2))
            .await
            .expect("known observation");
        observer
            .observe_usage(&first, &binding, Some(8), Some(2))
            .await
            .expect("duplicate cumulative observation");
        observer
            .dispatch_finished(&first, &binding)
            .await
            .expect("complete first response");
        let second = uuid::Uuid::new_v4().to_string();
        observer
            .before_dispatch(&second, &binding)
            .await
            .expect("physical retry");
        let partial = source_operation_usage(&storage, space, &ids)
            .await
            .expect("partial");
        assert_eq!(partial[0].physical_calls, 2);
        assert_eq!(partial[0].input_tokens, None);
        assert_eq!(partial[0].monetary_euros, None);
        assert!(!partial[0].complete);
        run.completed_at = Some(chrono::Utc::now());
        run.error = Some("timeout".into());
        runs.update(space, &run_id, run)
            .await
            .expect("failed outcome");
        items
            .update_running_for_worker(
                space,
                &item_id,
                &attempt.lease_token,
                WorkItemStatus::Failed,
                Some("timeout".into()),
            )
            .await
            .expect("fail original queue")
            .expect("owned");
        elephant_storage::work_retention::archive_terminal_work(
            &storage,
            chrono::Utc::now() + chrono::Duration::seconds(1),
            100,
        )
        .await
        .expect("archive history");
        observer
            .observe_usage(&second, &binding, Some(4), Some(0))
            .await
            .expect("late accounting only");
        let unfinished = source_operation_usage(&storage, space, &ids)
            .await
            .expect("late partial");
        assert_eq!(unfinished[0].input_tokens, None);
        assert_eq!(unfinished[0].output_tokens, None);
        assert!(!unfinished[0].complete);
        observer
            .dispatch_finished(&second, &binding)
            .await
            .expect("late terminal receipt");
        let known = source_operation_usage(&storage, space, &ids)
            .await
            .expect("archive and late usage");
        assert_eq!(known[0].physical_calls, 2);
        assert_eq!(known[0].input_tokens, Some(12));
        assert_eq!(known[0].output_tokens, Some(2));
        assert_eq!(known[0].monetary_euros, None);
        assert!(
            !known[0].complete,
            "queue outcome never completes source owner"
        );
        assert!(source_operation_usage(&storage, "foreign", &ids)
            .await
            .is_err());
        assert!(
            source_operation_usage(&storage, space, &[ids[0].clone(), ids[0].clone()])
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn embedded_original_operation_usage_preserves_unknown_and_archived_receipts() {
        contract(StorageClient::new_embedded().await.expect("embedded")).await;
    }
    #[tokio::test]
    async fn surreal_original_operation_usage_preserves_unknown_and_archived_receipts() {
        contract(StorageClient::new_memory().await.expect("memory")).await;
    }

    #[test]
    fn zero_requires_closed_execution_evidence_and_tokens_never_imply_euros() {
        assert_eq!(
            Aggregate::new(true).finish("op".into()).monetary_euros,
            None
        );
        let mut zero = Aggregate::new(true);
        zero.seen_runs = 1;
        assert_eq!(zero.finish("op".into()).monetary_euros, Some(0.0));
        let mut measured = Aggregate::new(true);
        measured.seen_runs = 1;
        measured.calls = 1;
        measured.input = Some(8);
        measured.output = Some(2);
        let result = measured.finish("op".into());
        assert!(result.complete);
        assert_eq!(result.monetary_euros, None);
        assert!(add(Some(u64::MAX), Some(1)).is_err());
    }
}
