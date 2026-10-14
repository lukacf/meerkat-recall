//! Attempt/background accounting sink installed at actual provider dispatch.

use crate::scheduler::execution_contract::{AttemptUsage, UsageBasis};
use crate::{PipelineError, PipelineResult};
use async_trait::async_trait;
use elephant_embeddings::governed::ProviderDispatchObserver;
use elephant_embeddings::{EmbeddingError, EmbeddingResult};
use elephant_storage::provider_usage_repository::{ProviderCallReceipt, ProviderUsageRepository};
use elephant_storage::StorageClient;
use elephant_types::provider_binding::ModelBindingProvenance;
use elephant_types::publication::AttemptFence;
use std::sync::Arc;

#[path = "source_operation_usage.rs"]
mod source_operation_usage;
pub use source_operation_usage::{source_operation_usage, SourceOperationUsage};

/// Dispatch observer that persists physical requests and measured usage.
#[derive(Debug)]
pub struct DurableUsageObserver {
    storage: StorageClient,
    repository: ProviderUsageRepository,
    space_id: String,
    scope_id: String,
    attempt: Option<AttemptFence>,
    writes: tokio::sync::Mutex<()>,
}

impl DurableUsageObserver {
    /// Bind an observer to the originally claimed attempt and its work-run receipt.
    #[must_use]
    pub fn for_attempt(
        storage: StorageClient,
        work_run_id: &elephant_types::RecordId,
        attempt: AttemptFence,
    ) -> Arc<Self> {
        Arc::new(Self {
            repository: ProviderUsageRepository::new(storage.clone()),
            storage,
            space_id: attempt.space_id.clone(),
            scope_id: work_run_id.to_string(),
            attempt: Some(attempt),
            writes: tokio::sync::Mutex::new(()),
        })
    }

    /// Trusted background units use an independent durable scope. They never
    /// impersonate queue attempts or grant publication authority.
    pub fn for_activity(storage: StorageClient, space_id: &str, kind: &str) -> Arc<Self> {
        Arc::new(Self {
            repository: ProviderUsageRepository::new(storage.clone()),
            storage,
            space_id: space_id.to_string(),
            scope_id: format!("{kind}:{}", uuid::Uuid::now_v7()),
            attempt: None,
            writes: tokio::sync::Mutex::new(()),
        })
    }

    /// Aggregate cumulative physical-call observations without inventing consumption.
    ///
    /// # Errors
    /// Returns missing-receipt, token overflow, and storage errors.
    pub async fn summarize(&self, mut usage: AttemptUsage) -> PipelineResult<AttemptUsage> {
        let receipts = self
            .repository
            .for_scope(&self.space_id, &self.scope_id)
            .await?;
        if receipts.is_empty()
            && (usage.basis != UsageBasis::NotStarted
                || usage.measured_tokens.is_some_and(|tokens| tokens != 0)
                || usage.estimated_tokens.is_some())
        {
            return Err(PipelineError::InvalidState(
                "Previously dispatched usage receipts are missing".into(),
            ));
        }
        usage.measured_tokens = None;
        usage.basis = if receipts.is_empty() {
            UsageBasis::NotStarted
        } else {
            UsageBasis::Measured
        };
        let mut total = 0_u64;
        let mut measured = false;
        for receipt in receipts {
            if !receipt.usage_complete
                || receipt.input_tokens.is_none()
                || receipt.output_tokens.is_none()
            {
                usage.basis = UsageBasis::Unknown;
            }
            measured |= receipt.input_tokens.is_some() || receipt.output_tokens.is_some();
            total = total
                .checked_add(receipt.input_tokens.unwrap_or(0))
                .and_then(|n| n.checked_add(receipt.output_tokens.unwrap_or(0)))
                .ok_or_else(|| PipelineError::InvalidState("Provider usage overflow".into()))?;
        }
        if measured {
            usage.measured_tokens = Some(total);
        }
        Ok(usage)
    }
}

// The SDK exposes only a string callback error. Classify while the original
// typed error is still present; never infer authority from diagnostic wording.
fn callback_failure(error: PipelineError) -> EmbeddingError {
    use crate::scheduler::execution_contract::{classify_failure, FailureClass};
    let message = error.to_string();
    let marker = match classify_failure(&error.into()) {
        FailureClass::ConfigurationBlocked => "configuration_blocked:",
        FailureClass::PendingDependencies => "source_publication_incomplete:",
        FailureClass::LostLease => "stale_attempt:",
        FailureClass::StaleInput => "recall_dispatch_stale_input:",
        FailureClass::Timeout => "recall_dispatch_timeout:",
        FailureClass::Transient => "recall_dispatch_transient:",
        FailureClass::Terminal => "recall_dispatch_terminal:",
    };
    EmbeddingError::StorageError(format!("{marker} {message}"))
}

fn storage_callback_failure(error: elephant_storage::StorageError) -> EmbeddingError {
    callback_failure(PipelineError::StorageError(error))
}

#[async_trait]
impl ProviderDispatchObserver for DurableUsageObserver {
    async fn before_dispatch(
        &self,
        call_id: &str,
        provenance: &ModelBindingProvenance,
    ) -> EmbeddingResult<()> {
        let _guard = self.writes.lock().await;
        if let Some(attempt) = &self.attempt {
            use elephant_storage::Repository;
            let item = elephant_storage::WorkItemRepository::new(self.storage.clone())
                .get(&attempt.space_id, &attempt.work_item_id)
                .await
                .map_err(storage_callback_failure)?
                .ok_or_else(|| {
                    EmbeddingError::StorageError("stale_attempt: original work is missing".into())
                })?;
            let current = elephant_storage::work_attempt_repository::fence_from_claim(&item)
                .map_err(|error| EmbeddingError::StorageError(format!("stale_attempt: {error}")))?;
            if &current != attempt {
                return Err(EmbeddingError::StorageError(
                    "stale_attempt: original dispatch ownership changed".into(),
                ));
            }
            crate::reprocess::guard_physical_dispatch(&self.storage, &item)
                .await
                .map_err(callback_failure)?;
            let run_id = self.scope_id.parse().map_err(|_| {
                EmbeddingError::StorageError(
                    "recall_dispatch_terminal: invalid attempt usage scope".into(),
                )
            })?;
            crate::scheduler::extraction_repair::guard_physical_dispatch(
                &self.storage,
                &item,
                &run_id,
            )
            .await
            .map_err(callback_failure)?;
        }
        let now = chrono::Utc::now();
        self.repository
            .start(ProviderCallReceipt {
                call_id: call_id.to_string(),
                space_id: self.space_id.clone(),
                scope_id: self.scope_id.clone(),
                attempt: self.attempt.clone(),
                provenance: provenance.clone(),
                input_tokens: None,
                output_tokens: None,
                usage_complete: false,
                started_at: now,
                updated_at: now,
            })
            .await
            .map_err(storage_callback_failure)
    }
    async fn dispatch_finished(
        &self,
        call_id: &str,
        provenance: &ModelBindingProvenance,
    ) -> EmbeddingResult<()> {
        let _guard = self.writes.lock().await;
        self.repository
            .finish(&self.space_id, &self.scope_id, call_id, provenance)
            .await
            .map_err(storage_callback_failure)
    }

    async fn observe_usage(
        &self,
        call_id: &str,
        provenance: &ModelBindingProvenance,
        input: Option<u64>,
        output: Option<u64>,
    ) -> EmbeddingResult<()> {
        let _guard = self.writes.lock().await;
        self.repository
            .observe(
                &self.space_id,
                &self.scope_id,
                call_id,
                provenance,
                input,
                output,
            )
            .await
            .map_err(storage_callback_failure)
    }
}

/// Replay settlement after outcome persistence or restart. Unknown physical
/// requests retain their reservation; measured totals never grow from estimates.
///
/// # Errors
/// Returns invalid receipt, missing reservation, or settlement storage errors.
pub async fn reconcile_finished_usage(
    storage: &StorageClient,
    space: &str,
    run_id: &elephant_types::RecordId,
) -> PipelineResult<()> {
    use elephant_storage::{Repository, WorkRunRepository};
    use std::str::FromStr;
    let repository = WorkRunRepository::new(storage.clone());
    let Some(mut run) = repository.get(space, run_id).await? else {
        return Ok(());
    };
    if run.completed_at.is_none() {
        return Ok(());
    }
    let Some(prior) = AttemptUsage::from_metrics(&run.metrics)
        .map_err(|error| PipelineError::ValidationError(error.to_string()))?
    else {
        return Ok(());
    };
    let Some(ledger) = run
        .metrics
        .custom
        .get("cost_ledger_id")
        .and_then(serde_json::Value::as_str)
    else {
        return Ok(());
    };
    let ledger = elephant_types::RecordId::from_str(ledger)
        .map_err(|error| PipelineError::ValidationError(error.to_string()))?;
    let ledger_record =
        elephant_storage::cost_ledger_daily_repository::CostLedgerDailyRepository::new(
            storage.clone(),
        )
        .get(space, &ledger)
        .await?
        .ok_or_else(|| {
            PipelineError::InvalidState("Attempt cost reservation disappeared".into())
        })?;
    if ledger_record.total_runs > 0 {
        return Ok(());
    }
    let observer = DurableUsageObserver {
        repository: ProviderUsageRepository::new(storage.clone()),
        storage: storage.clone(),
        space_id: space.into(),
        scope_id: run_id.to_string(),
        attempt: None,
        writes: tokio::sync::Mutex::new(()),
    };
    let usage = observer.summarize(prior.clone()).await?;
    usage
        .write_metrics(&mut run.metrics)
        .map_err(|error| PipelineError::ValidationError(error.to_string()))?;
    let success = run.success;
    if usage != prior {
        repository.update(space, run_id, run).await?;
    }
    if usage.basis == UsageBasis::Unknown {
        return Ok(());
    }
    crate::scheduler::BudgetEnforcer::new(
        storage.clone(),
        elephant_types::records::ingest::Budget::default(),
        space.to_string(),
    )
    .reconcile_cost(
        &ledger,
        usage.accounted_units(),
        usage.measured_tokens.unwrap_or(0),
        success,
    )
    .await?;
    Ok(())
}

/// Exhaustive hot/archive replay in one explicit named space. The query API
/// pages by durable ID and refuses cursor nonprogress instead of truncation.
///
/// # Errors
/// Returns outcome, usage, settlement, or nonprogressing pagination errors.
pub async fn recover_usage(storage: &StorageClient, space: &str) -> PipelineResult<()> {
    use elephant_storage::{Repository, WorkRunRepository};
    let repository = WorkRunRepository::new(storage.clone());
    let mut cursor = None;
    loop {
        let runs = repository.list(space, 256, cursor.as_ref()).await?;
        if runs.is_empty() {
            break;
        }
        for run in &runs {
            if run.metrics.custom.contains_key("usage") {
                if run.completed_at.is_none() {
                    repository.finish_abandoned(run).await?;
                }
                reconcile_finished_usage(storage, space, &run.base.id).await?;
            }
        }
        let next = runs.last().map(|run| run.base.id.clone());
        if next == cursor {
            return Err(PipelineError::InvalidState(
                "Usage recovery cursor did not advance".into(),
            ));
        }
        cursor = next;
        if runs.len() < 256 {
            break;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used)]
    use super::*;
    use elephant_types::provider_binding::{ModelOperation, SpaceConfigToken};

    #[test]
    fn physical_observer_callback_preserves_typed_categories_through_sdk_wrapping() {
        use crate::scheduler::execution_contract::{classify_failure, FailureClass};
        use elephant_storage::StorageError;
        for (error, expected) in [
            (
                PipelineError::StorageError(StorageError::ConnectionError(
                    "configuration_blocked: diagnostic text".into(),
                )),
                FailureClass::Transient,
            ),
            (
                PipelineError::StorageError(StorageError::ErasureInProgress),
                FailureClass::Transient,
            ),
            (
                PipelineError::StorageError(StorageError::DataIntegrityError(
                    "timeout configuration_blocked: corrupt evidence".into(),
                )),
                FailureClass::Terminal,
            ),
            (
                PipelineError::LeaseConflict("original owner changed".into()),
                FailureClass::LostLease,
            ),
            (
                PipelineError::ConfigError("original producer missing".into()),
                FailureClass::ConfigurationBlocked,
            ),
            (
                PipelineError::InvalidInput(
                    "timeout configuration_blocked: malformed input".into(),
                ),
                FailureClass::Terminal,
            ),
        ] {
            let callback = callback_failure(error);
            let wrapped = format!("Invalid request: provider SDK authorizer: {callback}");
            assert_eq!(
                classify_failure(&PipelineError::EmbeddingError(callback).into()),
                expected
            );
            assert_eq!(
                classify_failure(&PipelineError::ExtractionError(wrapped).into()),
                expected
            );
        }
    }

    #[test]
    fn dispatch_epoch_conflict_keeps_bounded_retry_through_callback() {
        use crate::scheduler::execution_contract::{
            classify_failure, retry_action, FailureClass, RetryAction,
        };
        let diagnostic = "Provider dispatch inputs changed; reprepare the original work";
        let callback = callback_failure(PipelineError::StorageError(
            elephant_storage::StorageError::Conflict(diagnostic.into()),
        ));
        let sdk_message = format!("Invalid request: provider SDK authorizer: {callback}");
        for error in [
            PipelineError::EmbeddingError(callback),
            PipelineError::ExtractionError(sdk_message),
        ] {
            let class = classify_failure(&error.into());
            assert_eq!(class, FailureClass::Transient);
            assert_eq!(
                retry_action(class, 1, 3, 5, 120),
                RetryAction::Retry { delay_secs: 5 }
            );
            assert_eq!(retry_action(class, 3, 3, 5, 120), RetryAction::Fail);
        }
        // Human-readable wording must not make arbitrary invalid state retryable.
        let invalid = callback_failure(PipelineError::InvalidState(diagnostic.into()));
        assert_eq!(
            classify_failure(&PipelineError::EmbeddingError(invalid).into()),
            FailureClass::Terminal,
        );
    }

    #[test]
    fn missing_explicit_credential_is_recoverable_but_invalid_output_stays_terminal() {
        use crate::scheduler::execution_contract::{classify_failure, FailureClass};
        use elephant_embeddings::governed::{CredentialResolver, EnvironmentCredentials};
        for reference in [
            String::new(),
            format!(
                "RECALL_MISSING_TEST_CREDENTIAL_{}",
                uuid::Uuid::new_v4().simple()
            ),
        ] {
            let error = EnvironmentCredentials
                .resolve(&reference)
                .expect_err("explicit reference unavailable");
            assert_eq!(
                classify_failure(&PipelineError::EmbeddingError(error).into()),
                FailureClass::ConfigurationBlocked
            );
        }
        let malformed = PipelineError::EmbeddingError(EmbeddingError::ProviderError(
            "provider_authorization: provider returned invalid embedding shape".into(),
        ));
        assert_eq!(classify_failure(&malformed.into()), FailureClass::Terminal);
    }

    fn binding() -> ModelBindingProvenance {
        ModelBindingProvenance {
            config: SpaceConfigToken {
                space_id: "quiet".into(),
                config_id: "config".into(),
                generation: 1,
                policy_id: "policy".into(),
            },
            operation: ModelOperation::DocumentEmbedding,
            binding_id: "binding".into(),
            profile_id: "profile".into(),
        }
    }

    #[test]
    fn historical_receipt_without_terminal_marker_remains_incomplete() {
        let now = chrono::Utc::now();
        let receipt = ProviderCallReceipt {
            call_id: uuid::Uuid::new_v4().to_string(),
            space_id: "quiet".into(),
            scope_id: "historical".into(),
            attempt: None,
            provenance: binding(),
            input_tokens: Some(8),
            output_tokens: Some(2),
            usage_complete: true,
            started_at: now,
            updated_at: now,
        };
        let mut payload = serde_json::to_value(receipt).expect("receipt");
        payload
            .as_object_mut()
            .expect("object")
            .remove("usage_complete");
        let historical: ProviderCallReceipt =
            serde_json::from_value(payload).expect("legacy payload");
        assert!(!historical.usage_complete);
        assert_eq!(historical.input_tokens, Some(8));
        assert_eq!(historical.output_tokens, Some(2));
    }
    async fn accounting_contract(storage: StorageClient) {
        let observer = DurableUsageObserver::for_activity(storage.clone(), "quiet", "fixture");
        let reserved = AttemptUsage::reserved(100_000, 1.0);
        assert_eq!(
            observer
                .summarize(reserved.clone())
                .await
                .expect("not started")
                .basis,
            UsageBasis::NotStarted
        );
        for basis in [
            UsageBasis::Unknown,
            UsageBasis::Measured,
            UsageBasis::Estimated,
        ] {
            let mut prior = reserved.clone();
            prior.basis = basis;
            assert!(
                observer.summarize(prior).await.is_err(),
                "Missing prior consumption evidence cannot become zero"
            );
        }
        let mut inconsistent = reserved.clone();
        inconsistent.measured_tokens = Some(7);
        assert!(observer.summarize(inconsistent).await.is_err());
        let first = uuid::Uuid::new_v4().to_string();
        observer
            .before_dispatch(&first, &binding())
            .await
            .expect("dispatch");
        let unknown = observer.summarize(reserved.clone()).await.expect("timeout");
        assert_eq!(unknown.basis, UsageBasis::Unknown);
        assert_eq!(unknown.measured_tokens, None);
        observer
            .observe_usage(&first, &binding(), Some(4), Some(2))
            .await
            .expect("usage");
        observer
            .observe_usage(&first, &binding(), Some(4), Some(2))
            .await
            .expect("duplicate");
        observer
            .observe_usage(&first, &binding(), Some(3), None)
            .await
            .expect("older snapshot");
        let unfinished = observer
            .summarize(reserved.clone())
            .await
            .expect("partial stream");
        assert_eq!(unfinished.measured_tokens, Some(6));
        assert_eq!(unfinished.basis, UsageBasis::Unknown);
        observer
            .dispatch_finished(&first, &binding())
            .await
            .expect("terminal response");
        let measured = observer.summarize(reserved.clone()).await.expect("summary");
        assert_eq!(measured.measured_tokens, Some(6));
        assert_eq!(measured.basis, UsageBasis::Measured);
        let retry = uuid::Uuid::new_v4().to_string();
        observer
            .before_dispatch(&retry, &binding())
            .await
            .expect("physical retry");
        let partial = observer.summarize(reserved.clone()).await.expect("partial");
        assert_eq!(partial.basis, UsageBasis::Unknown);
        assert_eq!(partial.measured_tokens, Some(6));
        observer
            .observe_usage(&retry, &binding(), Some(3), Some(0))
            .await
            .expect("late receipt");
        for _ in 0..2 {
            observer
                .dispatch_finished(&retry, &binding())
                .await
                .expect("idempotent terminal receipt");
        }
        assert_eq!(
            observer
                .summarize(reserved)
                .await
                .expect("late summary")
                .measured_tokens,
            Some(9)
        );
        let mut wrong = binding();
        wrong.config.space_id = "foreign".into();
        assert!(observer
            .before_dispatch(&uuid::Uuid::new_v4().to_string(), &wrong)
            .await
            .is_err());
        let erased_id = format!("provider_call_receipt:{retry}");
        if let Some(store) = storage.embedded_store() {
            store
                .mutate(|tx| {
                    tx.hard_delete_extension_row("provider_call_receipt", &erased_id, "quiet")?;
                    Ok::<(), elephant_storage::StorageError>(())
                })
                .await
                .expect("physically remove receipt");
        } else {
            storage
                .query_bind_checked("DELETE $receipt;", elephant_storage::bindings! {
                    "receipt" => surrealdb::sql::Thing::from(("provider_call_receipt", retry.as_str())),
                })
                .await
                .expect("physically remove receipt");
        }
        assert!(
            observer
                .dispatch_finished(&retry, &binding())
                .await
                .is_err(),
            "late completion cannot recreate erased accounting payload"
        );
        assert!(observer
            .observe_usage(&retry, &binding(), Some(99), Some(99))
            .await
            .is_err());
        assert_eq!(
            observer
                .repository
                .for_scope("quiet", &observer.scope_id)
                .await
                .expect("remaining receipts")
                .len(),
            1
        );
    }
    #[tokio::test]
    async fn embedded_receipts_preserve_unknown_and_count_once() {
        accounting_contract(StorageClient::new_embedded().await.expect("embedded")).await;
    }
    #[tokio::test]
    async fn surreal_receipts_preserve_unknown_and_count_once() {
        accounting_contract(StorageClient::new_memory().await.expect("memory")).await;
    }
    async fn finite_job_dispatch_contract(storage: StorageClient) {
        use elephant_storage::{
            Repository, ReprocessJobRepository, WorkItemRepository, WorkRunRepository,
        };
        use elephant_types::records::{
            base::BaseFields,
            reprocess::{
                ReprocessJob, ReprocessProgress, ReprocessStatus, ReprocessTargetStage,
                SelectionCriteria,
            },
            work::{WorkRun, WorkRunMetrics},
        };
        use elephant_types::{Link, RecordId, TableName};
        let jobs = ReprocessJobRepository::new(storage.clone());
        let job = jobs
            .create_with_work(ReprocessJob {
                base: BaseFields::new(
                    RecordId::new_v4(TableName::ReprocessJob),
                    "quiet".into(),
                    "fixture".into(),
                ),
                name: "finite original job".into(),
                selection_criteria: SelectionCriteria::default(),
                target_stages: vec![ReprocessTargetStage::ReResolve],
                status: ReprocessStatus::Pending,
                progress: ReprocessProgress::default(),
                priority: 1,
                budget_limit_euros: Some(5.0),
                max_sources_per_day: None,
                started_at: None,
                completed_at: None,
                error: None,
                reason: None,
            })
            .await
            .expect("durable job and original handoff");
        let id = jobs.ensure_work(&job).await.expect("original handoff ID");
        let item = WorkItemRepository::new(storage.clone())
            .claim_with_lease(
                "quiet",
                &id,
                "original",
                chrono::Utc::now() + chrono::Duration::minutes(5),
            )
            .await
            .expect("claim")
            .expect("available");
        let attempt = elephant_storage::work_attempt_repository::fence_from_claim(&item)
            .expect("original fence");
        let run_id = RecordId::new_v4(TableName::WorkRun);
        let mut metrics = WorkRunMetrics::default();
        AttemptUsage::reserved(100, 1.0)
            .write_metrics(&mut metrics)
            .expect("reservation");
        metrics.custom.insert(
            "attempt_fence".into(),
            serde_json::to_value(&attempt).expect("fence"),
        );
        WorkRunRepository::new(storage.clone())
            .create(WorkRun {
                base: BaseFields::new(run_id.clone(), "quiet".into(), "fixture".into()),
                work_item: Link::new(id),
                attempt: item.attempts,
                started_at: chrono::Utc::now(),
                completed_at: None,
                success: false,
                error: None,
                metrics,
                worker_id: "original".into(),
                worker_version: "fixture".into(),
                model_version: None,
                prompt_id: None,
                config_hash: None,
            })
            .await
            .expect("actual original work run");
        let observer = DurableUsageObserver::for_attempt(storage.clone(), &run_id, attempt);
        let refused_call = uuid::Uuid::new_v4().to_string();
        let error = observer
            .before_dispatch(&refused_call, &binding())
            .await
            .expect_err("no EUR quote for finite job");
        assert!(error.to_string().contains("configuration_blocked:"));
        assert!(
            observer
                .repository
                .for_scope("quiet", &run_id.to_string())
                .await
                .expect("receipts")
                .is_empty(),
            "a refused dispatch creates no physical call receipt"
        );
        assert_eq!(
            observer
                .summarize(AttemptUsage::reserved(100, 1.0))
                .await
                .expect("not sent")
                .basis,
            UsageBasis::NotStarted
        );
        let mut unbudgeted = job.clone();
        unbudgeted.budget_limit_euros = None;
        jobs.update("quiet", &job.base.id, unbudgeted)
            .await
            .expect("explicit current job policy change");
        observer
            .before_dispatch(&uuid::Uuid::new_v4().to_string(), &binding())
            .await
            .expect_err("original finite restriction survives current None");
        assert_eq!(
            observer
                .repository
                .for_scope("quiet", &run_id.to_string())
                .await
                .expect("receipts")
                .len(),
            0
        );
    }

    #[tokio::test]
    async fn embedded_finite_job_guard_runs_before_physical_receipt() {
        finite_job_dispatch_contract(StorageClient::new_embedded().await.expect("embedded")).await;
    }
    #[tokio::test]
    async fn surreal_finite_job_guard_runs_before_physical_receipt() {
        let storage = StorageClient::new_memory().await.expect("memory");
        elephant_migrations::apply_schema(&storage)
            .await
            .expect("explicit complete canonical schema");
        finite_job_dispatch_contract(storage).await;
    }

    async fn restart_settlement_contract(storage: StorageClient, interrupted: bool) {
        use elephant_storage::{Repository, WorkRunRepository};
        use elephant_types::records::base::BaseFields;
        use elephant_types::records::work::{WorkItem, WorkItemStatus, WorkRun, WorkRunMetrics};
        use elephant_types::{Link, RecordId, TableName};
        let budget = crate::scheduler::BudgetEnforcer::new(
            storage.clone(),
            elephant_types::records::ingest::Budget::default(),
            "quiet".into(),
        );
        let ledger = budget
            .reserve_cost(
                elephant_types::records::ingest::IngestMode::Graph,
                "fixture",
                0.001,
            )
            .await
            .expect("reserve");
        let run_id = RecordId::new_v4(TableName::WorkRun);
        let items = elephant_storage::WorkItemRepository::new(storage.clone());
        let queued = items
            .create(WorkItem {
                base: BaseFields::new(
                    RecordId::new_v4(TableName::WorkItem),
                    "quiet".into(),
                    "fixture".into(),
                ),
                kind: "fixture".into(),
                payload: serde_json::json!({}),
                status: WorkItemStatus::Runnable,
                priority: 1,
                idempotency_key: "receipt-fixture".into(),
                attempts: 0,
                lease_expires_at: None,
                leased_by: None,
                run_after: None,
                last_error: None,
            })
            .await
            .expect("queue");
        let claimed = items
            .claim_with_lease(
                "quiet",
                &queued.base.id,
                "original",
                chrono::Utc::now() + chrono::Duration::minutes(5),
            )
            .await
            .expect("claim")
            .expect("runnable");
        let attempt =
            elephant_storage::work_attempt_repository::fence_from_claim(&claimed).expect("fence");
        let observer = DurableUsageObserver::for_attempt(storage.clone(), &run_id, attempt.clone());
        let call = uuid::Uuid::new_v4().to_string();
        observer
            .before_dispatch(&call, &binding())
            .await
            .expect("physical request");
        let usage = observer
            .summarize(AttemptUsage::reserved(1000, 0.001))
            .await
            .expect("unknown");
        let mut metrics = WorkRunMetrics::default();
        usage.write_metrics(&mut metrics).expect("receipt");
        metrics.custom.insert(
            "attempt_fence".into(),
            serde_json::to_value(&attempt).expect("fence JSON"),
        );
        metrics.custom.insert(
            "cost_ledger_id".into(),
            serde_json::json!(ledger.to_string()),
        );
        WorkRunRepository::new(storage.clone())
            .create(WorkRun {
                base: BaseFields::new(run_id.clone(), "quiet".into(), "fixture".into()),
                work_item: Link::new(claimed.base.id.clone()),
                attempt: 1,
                started_at: chrono::Utc::now(),
                completed_at: (!interrupted).then(chrono::Utc::now),
                success: false,
                error: Some("deadline".into()),
                metrics,
                worker_id: "fixture".into(),
                worker_version: "fixture".into(),
                model_version: None,
                prompt_id: None,
                config_hash: None,
            })
            .await
            .expect("failed run");
        if interrupted {
            recover_usage(&storage, "quiet")
                .await
                .expect("live recovery");
            assert!(
                WorkRunRepository::new(storage.clone())
                    .get("quiet", &run_id)
                    .await
                    .expect("run")
                    .expect("present")
                    .completed_at
                    .is_none(),
                "live attempt remains open"
            );
            items
                .requeue_with_delay(
                    "quiet",
                    &claimed.base.id,
                    &attempt.lease_token,
                    0,
                    Some("fixture lease loss".into()),
                )
                .await
                .expect("release original")
                .expect("owned");
            let successor = items
                .claim_with_lease(
                    "quiet",
                    &claimed.base.id,
                    "successor",
                    chrono::Utc::now() + chrono::Duration::minutes(5),
                )
                .await
                .expect("reclaim")
                .expect("available");
            assert_ne!(successor.leased_by, claimed.leased_by);
            recover_usage(&storage, "quiet")
                .await
                .expect("interruption recovery");
            let recovered = WorkRunRepository::new(storage.clone())
                .get("quiet", &run_id)
                .await
                .expect("run")
                .expect("present");
            assert!(recovered.completed_at.is_some());
            assert!(!recovered.success);
            assert_eq!(
                recovered.metrics.custom.get("interrupted"),
                Some(&serde_json::json!(true))
            );
            assert_eq!(
                AttemptUsage::from_metrics(&recovered.metrics)
                    .expect("usage")
                    .expect("receipt")
                    .basis,
                UsageBasis::Unknown
            );
        }
        reconcile_finished_usage(&storage, "quiet", &run_id)
            .await
            .expect("retain uncertain reservation");
        let ledgers =
            elephant_storage::cost_ledger_daily_repository::CostLedgerDailyRepository::new(
                storage.clone(),
            );
        assert_eq!(
            ledgers
                .get("quiet", &ledger)
                .await
                .expect("ledger")
                .expect("exists")
                .total_runs,
            0
        );
        observer
            .observe_usage(&call, &binding(), Some(8), Some(2))
            .await
            .expect("late receipt");
        recover_usage(&storage, "quiet")
            .await
            .expect("partial recovery");
        assert_eq!(
            ledgers
                .get("quiet", &ledger)
                .await
                .expect("ledger")
                .expect("exists")
                .total_runs,
            0,
            "a partial cumulative snapshot cannot settle a timed-out reservation"
        );
        observer
            .dispatch_finished(&call, &binding())
            .await
            .expect("late terminal response");
        observer
            .dispatch_finished(&call, &binding())
            .await
            .expect("duplicate terminal response");
        recover_usage(&storage, "quiet")
            .await
            .expect("restart recovery");
        recover_usage(&storage, "quiet")
            .await
            .expect("repeat recovery");
        let reconciled = ledgers
            .get("quiet", &ledger)
            .await
            .expect("ledger")
            .expect("exists");
        assert_eq!(reconciled.total_runs, 1);
        assert_eq!(reconciled.total_tokens, 10);
    }
    #[tokio::test]
    async fn embedded_restart_settles_known_usage_once_and_holds_unknown() {
        restart_settlement_contract(
            StorageClient::new_embedded().await.expect("embedded"),
            false,
        )
        .await;
    }
    #[tokio::test]
    async fn surreal_restart_settles_known_usage_once_and_holds_unknown() {
        restart_settlement_contract(StorageClient::new_memory().await.expect("memory"), false)
            .await;
    }
    #[tokio::test]
    async fn embedded_abandoned_attempt_closes_only_old_run_and_holds_unknown_usage() {
        restart_settlement_contract(StorageClient::new_embedded().await.expect("embedded"), true)
            .await;
    }
    #[tokio::test]
    async fn surreal_abandoned_attempt_closes_only_old_run_and_holds_unknown_usage() {
        restart_settlement_contract(StorageClient::new_memory().await.expect("memory"), true).await;
    }
}
