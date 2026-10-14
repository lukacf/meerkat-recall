//! Execution service for durable remote source watches.

use chrono::{DateTime, Duration, Utc};
use elephant_storage::source_intake::{IntakeControl, PreparedSourceIntake};
use elephant_storage::source_publication::SourcePublicationRepository;
use elephant_storage::source_watch_producer::PreparedSourceWatchProducer;
use elephant_storage::source_watch_repository::PreparedSourceWatchObservation;
use elephant_storage::{
    Repository, SourceRepository, SourceWatchRepository, StorageClient, WorkItemRepository,
};
use elephant_types::publication::{
    AttemptFence, OperationReceipt, PublicationFence, SourceAuthorizationSnapshot, SourceVisibility,
};
use elephant_types::records::{
    Source, SourceType, SourceWatch, SourceWatchCheck, SourceWatchOutcome, SourceWatchStatus,
    WorkItemStatus,
};
use elephant_types::{Link, RecordId, TableName};
use sha2::{Digest, Sha256};

use crate::error::{PipelineError, PipelineResult};
use crate::remote_source::{
    fetch_remote_source, RemoteFetchOutcome, RemoteFetchPolicy, RemoteSnapshot, RemoteValidators,
};
use crate::source_queue::{enqueue_accepted_source, enqueue_accepted_source_from_attempt};

#[path = "source_watch_local.rs"]
mod local;

const MAX_RECORDED_ERROR_BYTES: usize = 2048;
const MAX_RETRY_SECONDS: i64 = 24 * 60 * 60;

/// Durable work kind that executes one source-watch observation.
pub const REFRESH_SOURCE_WATCH_KIND: &str = "refresh_source_watch";

/// Schedule the current durable slot under its existing explicit delegation.
/// Actor strings identify logs only; the persisted registration fixes authority
/// and the original queue actor. Same-slot retries preserve original ancestry.
pub async fn enqueue_source_watch_refresh(
    storage: &StorageClient,
    watch: &SourceWatch,
    actor: &str,
    priority: i32,
) -> PipelineResult<RecordId> {
    validate_watch(watch)?;
    let standing =
        elephant_storage::standing_watch_repository::StandingWatchRepository::new(storage.clone())
            .capture_for_scheduling(watch)
            .await?;
    let source = SourceRepository::new(storage.clone())
        .get(&watch.base.space_id, watch.source.id())
        .await?
        .ok_or_else(|| PipelineError::NotFound(watch.source.id().to_string()))?;
    let records =
        elephant_storage::source_watch_registration::PreparedSourceWatchRegistration::prepare(
            storage,
            &source,
            Some(watch),
            watch.clone(),
        )
        .await?;
    enqueue_source_watch_refresh_authorized(storage, &records, &standing, None, actor, priority)
        .await
}

/// Schedule after actual current ingress and standing-owner preflight. Both
/// original read sets are checked by the queue owner in its committing mutation.
pub async fn enqueue_source_watch_refresh_authorized(
    storage: &StorageClient,
    records: &elephant_storage::source_watch_registration::PreparedSourceWatchRegistration,
    standing: &elephant_storage::standing_watch_repository::PreparedStandingWatchAuthority,
    invoker: Option<&elephant_auth::standing_watch::StandingWatchAdmission>,
    _actor: &str,
    priority: i32,
) -> PipelineResult<RecordId> {
    records.authorize(standing.principal())?;
    if let Some(invoker) = invoker {
        records.authorize(invoker.principal())?;
    }
    let item = WorkItemRepository::new(storage.clone())
        .enqueue_source_watch_refresh(records, standing, invoker, priority)
        .await?;
    Ok(item.base.id)
}

/// Result of one durable source watch execution.
#[derive(Debug, Clone, serde::Serialize)]
pub struct SourceWatchRun {
    /// Updated mutable watch.
    pub watch: SourceWatch,
    /// Immutable check receipt.
    pub check: SourceWatchCheck,
    /// Triage work scheduled after a content change.
    pub triage_work_item: Option<RecordId>,
}

/// Worker result keeps a read-only retry distinct from a newly recorded check.
/// This is execution context only, not a new public source state or wire DTO.
#[derive(Debug, Clone)]
#[expect(
    clippy::large_enum_variant,
    reason = "Preserve the existing worker result API and owned observation layout; receipt-only retries remain a distinct variant."
)]
pub enum SourceWatchAttemptOutcome {
    /// A real remote observation was durably recorded under the original claim.
    Observed(SourceWatchRun),
    /// The original source receipt was returned without staging or queue writes.
    ReceiptOnly(OperationReceipt),
}

#[derive(Debug, PartialEq)]
enum StagedWatchSource {
    Staged(Option<RecordId>),
    ReceiptOnly(OperationReceipt),
}

/// Executes recurring remote observations and source-first re-ingest staging.
#[derive(Clone, Debug)]
pub struct SourceWatchService {
    storage: StorageClient,
    watches: SourceWatchRepository,
    sources: SourceRepository,
    fetch_policy: RemoteFetchPolicy,
    local_admission: Option<elephant_auth::standing_watch::StandingWatchAdmission>,
    local_budget: elephant_types::records::ingest::Budget,
}

impl SourceWatchService {
    /// Creates a production watcher with a text-sized remote fetch budget.
    #[must_use]
    pub fn new(storage: StorageClient) -> Self {
        Self::with_fetch_policy(storage, RemoteFetchPolicy::web_watch())
    }

    /// Creates a watcher with an explicit fetch policy.
    ///
    /// This is useful for controlled private-network tests. Production callers
    /// should use [`Self::new`].
    #[must_use]
    pub fn with_fetch_policy(storage: StorageClient, fetch_policy: RemoteFetchPolicy) -> Self {
        Self {
            watches: SourceWatchRepository::new(storage.clone()),
            sources: SourceRepository::new(storage.clone()),
            storage,
            fetch_policy,
            local_admission: None,
            local_budget: elephant_types::records::ingest::Budget::default(),
        }
    }

    /// Explicit trusted library convenience using a real queued attempt. Signed
    /// admissions must use their registered service path instead.
    pub fn with_trusted_local_admission(
        storage: StorageClient,
        admission: elephant_auth::standing_watch::StandingWatchAdmission,
    ) -> PipelineResult<Self> {
        Self::with_trusted_local_fetch_policy(storage, admission, RemoteFetchPolicy::web_watch())
    }

    /// Explicit trusted library execution with a bounded caller-selected policy.
    pub fn with_trusted_local_fetch_policy(
        storage: StorageClient,
        admission: elephant_auth::standing_watch::StandingWatchAdmission,
        policy: RemoteFetchPolicy,
    ) -> PipelineResult<Self> {
        if !matches!(
            admission.origin(),
            elephant_auth::standing_watch::StandingWatchOrigin::TrustedLocal
        ) || policy.max_bytes == 0
            || policy.timeout.is_zero()
        {
            return Err(PipelineError::SecurityViolation(
                "explicit trusted local watch admission required".into(),
            ));
        }
        let mut service = Self::with_fetch_policy(storage, policy);
        service.local_admission = Some(admission);
        Ok(service)
    }

    /// Set the actual budget used by the local queued worker. Ordinary worker
    /// services retain their scheduler's own budget configuration.
    #[must_use]
    pub fn with_local_budget(mut self, budget: elephant_types::records::ingest::Budget) -> Self {
        self.local_budget = budget;
        self
    }

    /// Checks a watch now and always attempts to persist a conclusive receipt.
    ///
    /// Remote failures are represented as successful `Failed` receipts. Errors
    /// are returned only when Elephant cannot load or durably record state, or
    /// when changed content cannot be staged safely.
    pub async fn check(&self, watch: SourceWatch, actor: &str) -> PipelineResult<SourceWatchRun> {
        let admission = self.local_admission.as_ref().ok_or_else(|| {
            PipelineError::SecurityViolation("direct watch checks require explicit trusted-local admission; use queued service dispatch for signed registrations".into())
        })?;
        self.storage
            .with_activity(Box::pin(local::check(self, watch, actor, admission)))
            .await?
    }

    /// Capture the original claimed execution before fetching. Every source,
    /// document, initial queue and observation write checks that same guard.
    pub async fn check_with_attempt(
        &self,
        watch: SourceWatch,
        actor: &str,
        original_attempt: &AttemptFence,
        original_run_id: &RecordId,
    ) -> PipelineResult<SourceWatchAttemptOutcome> {
        let producer = Box::pin(PreparedSourceWatchProducer::capture(
            &self.storage,
            &watch,
            actor,
            original_attempt,
            original_run_id,
        ))
        .await?;
        Box::pin(self.check_mode(watch, actor, Some(&producer))).await
    }

    async fn check_mode(
        &self,
        watch: SourceWatch,
        actor: &str,
        producer: Option<&PreparedSourceWatchProducer>,
    ) -> PipelineResult<SourceWatchAttemptOutcome> {
        validate_watch(&watch)?;
        let started_at = Utc::now();
        let source = self
            .sources
            .get(&watch.base.space_id, watch.source.id())
            .await?
            .ok_or_else(|| PipelineError::NotFound(watch.source.id().to_string()))?;
        validate_security_envelope(&watch, &source)?;
        if let Some(producer) = producer {
            let records = elephant_storage::source_watch_registration::PreparedSourceWatchRegistration::prepare(
                &self.storage, &source, Some(&watch), watch.clone(),
            ).await?;
            records.authorize(producer.principal())?;
        }
        let authorized = SourceAuthorizationSnapshot::from(&source);
        let original_live_source = source.clone();
        // Capture intent before the network operation. A late response cannot
        // overwrite a generation accepted while the request was in flight.
        let publication_repo = SourcePublicationRepository::new(self.storage.clone());
        let head = publication_repo
            .head(&watch.base.space_id, &source.base.id)
            .await?
            .filter(|head| head.lifecycle == SourceVisibility::Active)
            .ok_or_else(|| {
                PipelineError::InvalidState("watch source has no active accepted head".into())
            })?;
        let desired_revision = head.desired_revision.as_deref().ok_or_else(|| {
            PipelineError::InvalidState("watch source has no accepted revision".into())
        })?;
        let source = publication_repo
            .revision(&watch.base.space_id, &source.base.id, desired_revision)
            .await?
            .ok_or_else(|| {
                PipelineError::InvalidState("watch source revision is unavailable".into())
            })?
            .source;
        validate_security_envelope(&watch, &source)?;
        let original = PublicationFence {
            source_id: head.source_id.clone(),
            generation: head.generation.clone(),
            revision_id: desired_revision.into(),
            operation_id: head.operation_id.clone(),
        };

        let input_observation = PreparedSourceWatchObservation::capture(
            &self.storage,
            &watch,
            &original_live_source,
            &head,
        )
        .await?;

        if source.type_ != SourceType::Web {
            return self
                .finish_unsupported(
                    watch,
                    started_at,
                    format!("source type {:?} is not watchable", source.type_),
                    producer,
                )
                .await
                .map(SourceWatchAttemptOutcome::Observed);
        }

        let validators = RemoteValidators {
            etag: watch.etag.clone(),
            last_modified: watch.last_modified.clone(),
        };
        let fetched =
            fetch_remote_source(&watch.requested_uri, &validators, self.fetch_policy).await;
        match fetched {
            Ok(RemoteFetchOutcome::NotModified {
                final_url,
                validators,
            }) => {
                let completed_at = Utc::now();
                let check = self.check_receipt(
                    &watch,
                    SourceWatchOutcome::NotModified,
                    started_at,
                    completed_at,
                    Some(final_url.to_string()),
                    None,
                    validators,
                    watch.observed_hash.clone(),
                    watch.observed_hash.clone(),
                    None,
                    None,
                );
                self.finish_success(watch, check, None, producer)
                    .await
                    .map(SourceWatchAttemptOutcome::Observed)
            }
            Ok(RemoteFetchOutcome::Snapshot(snapshot)) => {
                Box::pin(self.finish_snapshot(
                    watch,
                    source,
                    snapshot,
                    started_at,
                    actor,
                    original,
                    authorized,
                    producer,
                    Some(&input_observation),
                ))
                .await
            }
            Err(error) => {
                let completed_at = Utc::now();
                let message = bounded_error(error.to_string());
                let check = self.check_receipt(
                    &watch,
                    SourceWatchOutcome::Failed,
                    started_at,
                    completed_at,
                    None,
                    None,
                    validators,
                    watch.observed_hash.clone(),
                    None,
                    None,
                    Some(message.clone()),
                );
                self.finish_failure(watch, check, message, producer)
                    .await
                    .map(SourceWatchAttemptOutcome::Observed)
            }
        }
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "Keep original publication fence, authorization, producer and input observation separate from remotely observed snapshot bytes."
    )]
    async fn finish_snapshot(
        &self,
        watch: SourceWatch,
        source: Source,
        snapshot: RemoteSnapshot,
        started_at: DateTime<Utc>,
        actor: &str,
        original: PublicationFence,
        authorized: SourceAuthorizationSnapshot,
        producer: Option<&PreparedSourceWatchProducer>,
        input_observation: Option<&PreparedSourceWatchObservation>,
    ) -> PipelineResult<SourceWatchAttemptOutcome> {
        let completed_at = Utc::now();
        let observed_hash = sha256_hex(&snapshot.bytes);
        let prior_hash = watch
            .observed_hash
            .clone()
            .or_else(|| Some(source.hash.clone()));
        let content_length = u64::try_from(snapshot.bytes.len()).map_err(|_| {
            PipelineError::InvalidInput("remote source content length exceeds u64".to_string())
        })?;

        let matches_watch = watch.observed_hash.as_deref() == Some(observed_hash.as_str());
        if matches_watch || source.hash == observed_hash {
            // Acceptance may have survived a crash before the Watch was
            // advanced. Observe the already accepted bytes without minting a
            // replacement generation or adopting its receipt as new authority.
            let original_input = if matches_watch {
                None
            } else {
                Some(input_observation.ok_or_else(|| {
                    PipelineError::InvalidState("missing original watch input observation".into())
                })?)
            };
            let check = self.check_receipt(
                &watch,
                SourceWatchOutcome::HashUnchanged,
                started_at,
                completed_at,
                Some(snapshot.final_url.to_string()),
                snapshot.content_type,
                snapshot.validators,
                prior_hash,
                Some(observed_hash),
                Some(content_length),
                None,
            );
            return self
                .finish_success_with_observation(watch, check, None, producer, original_input)
                .await
                .map(SourceWatchAttemptOutcome::Observed);
        }

        let Some(content_type) = snapshot.content_type.clone() else {
            return self
                .finish_unsupported(
                    watch,
                    started_at,
                    "remote source omitted Content-Type".to_string(),
                    producer,
                )
                .await
                .map(SourceWatchAttemptOutcome::Observed);
        };
        if !is_supported_text_type(&content_type) {
            return self
                .finish_unsupported(
                    watch,
                    started_at,
                    format!("remote media type {content_type} is not yet watchable"),
                    producer,
                )
                .await
                .map(SourceWatchAttemptOutcome::Observed);
        }
        let text = match String::from_utf8(snapshot.bytes) {
            Ok(text) => text,
            Err(error) => {
                let message = bounded_error(format!("remote source is not valid UTF-8: {error}"));
                let check = self.check_receipt(
                    &watch,
                    SourceWatchOutcome::Failed,
                    started_at,
                    completed_at,
                    Some(snapshot.final_url.to_string()),
                    Some(content_type),
                    snapshot.validators,
                    prior_hash,
                    Some(observed_hash),
                    Some(content_length),
                    Some(message.clone()),
                );
                return self
                    .finish_failure(watch, check, message, producer)
                    .await
                    .map(SourceWatchAttemptOutcome::Observed);
            }
        };

        let staged = Box::pin(self.stage_changed_text_source(
            &source,
            &content_type,
            snapshot.final_url.as_str(),
            &observed_hash,
            text,
            actor,
            &original,
            authorized,
            producer,
        ))
        .await;
        let triage_work_item = match staged {
            Ok(StagedWatchSource::Staged(work_item)) => work_item,
            Ok(StagedWatchSource::ReceiptOnly(receipt)) => {
                return Ok(SourceWatchAttemptOutcome::ReceiptOnly(receipt));
            }
            // An obsolete or action-required attempt must not convert refusal
            // into an apparently successful new watch receipt. Queue recovery
            // retains ownership of any accepted-before-enqueue crash gap.
            Err(error) if producer.is_some() => return Err(error),
            Err(error) => {
                let message = bounded_error(error.to_string());
                let check = self.check_receipt(
                    &watch,
                    SourceWatchOutcome::Failed,
                    started_at,
                    completed_at,
                    Some(snapshot.final_url.to_string()),
                    Some(content_type),
                    snapshot.validators,
                    prior_hash,
                    Some(observed_hash),
                    Some(content_length),
                    Some(message.clone()),
                );
                return self
                    .finish_failure(watch, check, message, producer)
                    .await
                    .map(SourceWatchAttemptOutcome::Observed);
            }
        };
        let check = self.check_receipt(
            &watch,
            SourceWatchOutcome::Changed,
            started_at,
            completed_at,
            Some(snapshot.final_url.to_string()),
            Some(content_type),
            snapshot.validators,
            prior_hash,
            Some(observed_hash),
            Some(content_length),
            None,
        );
        self.finish_success(watch, check, triage_work_item, producer)
            .await
            .map(SourceWatchAttemptOutcome::Observed)
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "Keep original publication and producer authority explicit while staging the observed content and metadata."
    )]
    async fn stage_changed_text_source(
        &self,
        source: &Source,
        content_type: &str,
        final_uri: &str,
        hash: &str,
        text: String,
        actor: &str,
        original: &PublicationFence,
        authorized: SourceAuthorizationSnapshot,
        producer: Option<&PreparedSourceWatchProducer>,
    ) -> PipelineResult<StagedWatchSource> {
        let mut template = source
            .manual_doc_template()
            .map_err(|error| PipelineError::InvalidInput(error.to_string()))?
            .ok_or_else(|| {
                PipelineError::InvalidState(
                    "watched source has no accepted document template".into(),
                )
            })?;
        template.text = text;
        template.sidecar_text = None;
        template.media_attachments.clear();
        let metadata = template.metadata.as_object_mut().ok_or_else(|| {
            PipelineError::InvalidState("watched document metadata must be an object".into())
        })?;
        metadata.insert(
            "content_type".into(),
            serde_json::Value::String(content_type.into()),
        );
        metadata.insert(
            "source_type".into(),
            serde_json::Value::String(content_type.into()),
        );
        metadata.insert(
            "final_url".into(),
            serde_json::Value::String(final_uri.into()),
        );
        metadata.insert(
            "source_watch_observed_hash".into(),
            serde_json::Value::String(hash.into()),
        );
        let mut incoming = source.clone();
        incoming.hash = hash.into();
        incoming
            .set_manual_doc_template(&template)
            .map_err(|error| PipelineError::InvalidInput(error.to_string()))?;
        let revision = format!("watch:{}:{hash}", original.generation);
        let prepared = Box::pin(PreparedSourceIntake::prepare(
            &self.storage,
            incoming,
            IntakeControl {
                revision_id: Some(revision.clone()),
                idempotency_key: Some(format!("source-watch:{}:{revision}", source.base.id)),
                expected_generation: Some(original.generation.clone()),
            },
        ))
        .await?;
        prepared.validate_service_authority(&authorized)?;
        if let Some(producer) = producer {
            use elephant_storage::source_watch_registration::authorize_standing_watch_record;
            let principal = producer.principal();
            authorize_standing_watch_record(
                principal,
                &prepared.source,
                ["read:source", "write:source"],
            )?;
            if let Some(previous) = &prepared.previous {
                authorize_standing_watch_record(
                    principal,
                    previous,
                    ["read:source", "write:source"],
                )?;
            }
            for doc in prepared
                .previous_documents
                .iter()
                .chain(prepared.document.iter())
            {
                authorize_standing_watch_record(principal, doc, ["read:doc", "write:doc"])?;
            }
        }
        if Box::pin(prepared.read_only_contact_retry(&self.storage))
            .await?
            .is_some()
        {
            return Err(PipelineError::InvalidState(
                "accepted_contact_plan_unavailable".into(),
            ));
        }
        let contact = Box::pin(prepared.prepare_reprocess_contact(&self.storage, original))
            .await
            .map_err(watch_intake_error)?;
        if let Some(contact) = &contact {
            contact.validate_service_authority(&authorized)?;
            if let Some(producer) = producer {
                for record in contact.authorizations() {
                    elephant_storage::source_watch_registration::authorize_standing_watch_record(
                        producer.principal(),
                        record,
                        [record.read_scope(), record.write_scope()],
                    )?;
                }
            }
        }
        let prepared = prepared.with_authorized_snapshot(authorized);
        let accepted = if let Some(producer) = producer {
            Box::pin(prepared.accept_with_watch_producer(&self.storage, producer, contact.as_ref()))
                .await
                .map_err(watch_intake_error)?
        } else {
            match &contact {
                Some(contact) => {
                    Box::pin(prepared.accept_with_prepared_contact(&self.storage, contact)).await?
                }
                None => Box::pin(prepared.accept(&self.storage)).await?,
            }
        };
        if !accepted.needs_processing() {
            return Ok(if producer.is_some() {
                StagedWatchSource::ReceiptOnly(accepted.operation)
            } else {
                StagedWatchSource::Staged(None)
            });
        }
        // Acceptance survives a queue outage. All-space recovery discovers it;
        // a watch failure receipt must never undo the accepted generation.
        let work = if let Some(producer) = producer {
            Box::pin(enqueue_accepted_source_from_attempt(
                &self.storage,
                &accepted.source,
                &accepted.publication,
                actor,
                producer,
            ))
            .await?
        } else {
            Box::pin(enqueue_accepted_source(
                &self.storage,
                &accepted.source,
                &accepted.publication,
                actor,
            ))
            .await?
        };
        Ok(StagedWatchSource::Staged(Some(work)))
    }

    #[allow(clippy::too_many_arguments)]
    fn check_receipt(
        &self,
        watch: &SourceWatch,
        outcome: SourceWatchOutcome,
        started_at: DateTime<Utc>,
        completed_at: DateTime<Utc>,
        final_uri: Option<String>,
        content_type: Option<String>,
        validators: RemoteValidators,
        prior_hash: Option<String>,
        observed_hash: Option<String>,
        content_length: Option<u64>,
        error: Option<String>,
    ) -> SourceWatchCheck {
        let mut base = watch.base.clone();
        base.id = RecordId::new_v7(TableName::SourceWatchCheck);
        base.created_at = completed_at;
        base.updated_at = completed_at;
        base.deleted_at = None;
        SourceWatchCheck {
            base,
            watch: Link::new(watch.base.id.clone()),
            source: watch.source.clone(),
            outcome,
            started_at,
            completed_at,
            requested_uri: watch.requested_uri.clone(),
            final_uri,
            content_type,
            etag: validators.etag,
            last_modified: validators.last_modified,
            prior_hash,
            observed_hash,
            content_length,
            error,
        }
    }

    async fn record_check(
        &self,
        watch: SourceWatch,
        check: SourceWatchCheck,
        producer: Option<&PreparedSourceWatchProducer>,
    ) -> PipelineResult<(SourceWatch, SourceWatchCheck)> {
        match producer {
            Some(producer) => {
                self.watches
                    .record_check_with_watch_producer(watch, check, producer)
                    .await
            }
            None => self.watches.record_check(watch, check).await,
        }
        .map_err(Into::into)
    }

    async fn finish_success(
        &self,
        watch: SourceWatch,
        check: SourceWatchCheck,
        triage_work_item: Option<RecordId>,
        producer: Option<&PreparedSourceWatchProducer>,
    ) -> PipelineResult<SourceWatchRun> {
        self.finish_success_with_observation(watch, check, triage_work_item, producer, None)
            .await
    }

    async fn finish_success_with_observation(
        &self,
        mut watch: SourceWatch,
        check: SourceWatchCheck,
        triage_work_item: Option<RecordId>,
        producer: Option<&PreparedSourceWatchProducer>,
        input_observation: Option<&PreparedSourceWatchObservation>,
    ) -> PipelineResult<SourceWatchRun> {
        watch.status = SourceWatchStatus::Enabled;
        watch.last_checked_at = Some(check.completed_at);
        if check.outcome == SourceWatchOutcome::Changed {
            watch.last_changed_at = Some(check.completed_at);
        }
        watch.etag.clone_from(&check.etag);
        watch.last_modified.clone_from(&check.last_modified);
        if check.observed_hash.is_some() {
            watch.observed_hash.clone_from(&check.observed_hash);
        }
        watch.consecutive_failures = 0;
        watch.last_error = None;
        watch.next_check_at = next_check_at(check.completed_at, watch.interval_seconds)?;
        let (watch, check) = match input_observation {
            Some(observation) => {
                self.watches
                    .record_check_with_input_observation(watch, check, producer, observation)
                    .await?
            }
            None => self.record_check(watch, check, producer).await?,
        };
        Ok(SourceWatchRun {
            watch,
            check,
            triage_work_item,
        })
    }

    async fn finish_failure(
        &self,
        mut watch: SourceWatch,
        check: SourceWatchCheck,
        message: String,
        producer: Option<&PreparedSourceWatchProducer>,
    ) -> PipelineResult<SourceWatchRun> {
        watch.status = SourceWatchStatus::Error;
        watch.last_checked_at = Some(check.completed_at);
        watch.consecutive_failures = watch.consecutive_failures.saturating_add(1);
        watch.last_error = Some(message);
        let exponent = watch.consecutive_failures.saturating_sub(1).min(20);
        let multiplier = 1_i64.checked_shl(exponent).unwrap_or(i64::MAX);
        let retry_seconds = watch
            .interval_seconds
            .saturating_mul(multiplier)
            .min(MAX_RETRY_SECONDS);
        watch.next_check_at = next_check_at(check.completed_at, retry_seconds)?;
        let (watch, check) = self.record_check(watch, check, producer).await?;
        Ok(SourceWatchRun {
            watch,
            check,
            triage_work_item: None,
        })
    }

    async fn finish_unsupported(
        &self,
        mut watch: SourceWatch,
        started_at: DateTime<Utc>,
        message: String,
        producer: Option<&PreparedSourceWatchProducer>,
    ) -> PipelineResult<SourceWatchRun> {
        let completed_at = Utc::now();
        let message = bounded_error(message);
        let check = self.check_receipt(
            &watch,
            SourceWatchOutcome::Unsupported,
            started_at,
            completed_at,
            None,
            None,
            RemoteValidators {
                etag: watch.etag.clone(),
                last_modified: watch.last_modified.clone(),
            },
            watch.observed_hash.clone(),
            None,
            None,
            Some(message.clone()),
        );
        watch.status = SourceWatchStatus::Unsupported;
        watch.last_checked_at = Some(completed_at);
        watch.last_error = Some(message);
        watch.next_check_at = next_check_at(completed_at, watch.interval_seconds)?;
        let (watch, check) = self.record_check(watch, check, producer).await?;
        Ok(SourceWatchRun {
            watch,
            check,
            triage_work_item: None,
        })
    }
}

// This exact owner-defined refusal is terminal for the original intent. Other
// typed storage errors, including stale_attempt, retain B's normal classifier.
fn watch_intake_error(error: elephant_storage::StorageError) -> PipelineError {
    match error {
        elephant_storage::StorageError::Conflict(ref category)
            if category == "accepted_contact_plan_unavailable" =>
        {
            PipelineError::InvalidState(category.clone())
        }
        other => other.into(),
    }
}

fn validate_watch(watch: &SourceWatch) -> PipelineResult<()> {
    if watch.interval_seconds <= 0 {
        return Err(PipelineError::InvalidInput(
            "source watch interval_seconds must be positive".to_string(),
        ));
    }
    if watch.source.id().table() != TableName::Source {
        return Err(PipelineError::InvalidInput(
            "source watch must reference a source record".to_string(),
        ));
    }
    Ok(())
}

fn validate_security_envelope(watch: &SourceWatch, source: &Source) -> PipelineResult<()> {
    if watch.base.space_id != source.base.space_id
        || watch.base.security != source.base.security
        || watch.base.subjects != source.base.subjects
        || watch.base.subjects_state != source.base.subjects_state
    {
        return Err(PipelineError::SecurityViolation(
            "source watch security envelope does not match its source".to_string(),
        ));
    }
    Ok(())
}

fn is_supported_text_type(content_type: &str) -> bool {
    content_type.starts_with("text/")
        || matches!(
            content_type,
            "application/json"
                | "application/ld+json"
                | "application/xml"
                | "application/xhtml+xml"
                | "application/javascript"
        )
}

fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

fn next_check_at(at: DateTime<Utc>, seconds: i64) -> PipelineResult<DateTime<Utc>> {
    at.checked_add_signed(Duration::seconds(seconds))
        .ok_or_else(|| {
            PipelineError::InvalidInput("source watch next_check_at overflow".to_string())
        })
}

fn bounded_error(mut message: String) -> String {
    if message.len() <= MAX_RECORDED_ERROR_BYTES {
        return message;
    }
    let mut boundary = MAX_RECORDED_ERROR_BYTES;
    while !message.is_char_boundary(boundary) {
        boundary -= 1;
    }
    message.truncate(boundary);
    message
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used)]

    use super::*;
    use elephant_storage::{DocRepository, SpaceConfigRepository};
    use elephant_types::records::base::BaseFields;
    use elephant_types::records::{Doc, SourceLifecycleStatus, SpaceConfig, WorkItem};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[test]
    fn media_type_gate_is_explicit() {
        assert!(is_supported_text_type("text/html"));
        assert!(is_supported_text_type("application/ld+json"));
        assert!(!is_supported_text_type("application/pdf"));
        assert!(!is_supported_text_type("image/png"));
    }

    #[test]
    fn recorded_errors_are_utf8_bounded() {
        let value = "a".repeat(MAX_RECORDED_ERROR_BYTES - 1) + "éé";
        let bounded = bounded_error(value);
        assert!(bounded.len() <= MAX_RECORDED_ERROR_BYTES);
        assert!(bounded.is_char_boundary(bounded.len()));
    }

    fn local_admission(
        watch: &SourceWatch,
    ) -> elephant_auth::standing_watch::StandingWatchAdmission {
        let principal = elephant_policy::Principal {
            space_id: watch.base.space_id.clone(),
            scopes: [
                "read:source",
                "write:source",
                "read:doc",
                "write:doc",
                "read:entity",
                "write:entity",
                "read:relationship",
                "write:relationship",
            ]
            .into_iter()
            .map(str::to_owned)
            .collect(),
            clearances: vec![elephant_policy::Clearance {
                policy_id: watch.base.security.policy_id.clone(),
                max_level: watch.base.security.level,
                labels: watch.base.security.labels.iter().cloned().collect(),
                handling_allow: watch.base.security.handling.iter().cloned().collect(),
            }],
            subject_allowlist: None,
            purpose: Default::default(),
        };
        elephant_auth::standing_watch::StandingWatchAdmission::trusted_local(&principal)
            .expect("explicit local admission")
    }

    async fn register_local_watch(storage: &StorageClient, source: &Source, watch: &SourceWatch) {
        let admission = local_admission(watch);
        let source = SourceRepository::new(storage.clone())
            .get(&watch.base.space_id, &source.base.id)
            .await
            .expect("current source")
            .expect("source");
        let registration = Box::pin(
            elephant_storage::source_watch_registration::PreparedSourceWatchRegistration::prepare(
                storage,
                &source,
                Some(watch),
                watch.clone(),
            ),
        )
        .await
        .expect("original records");
        let standing = Box::pin(
            elephant_storage::standing_watch_repository::StandingWatchRepository::new(
                storage.clone(),
            )
            .prepare_registration(watch, &admission),
        )
        .await
        .expect("standing registration");
        Box::pin(registration.register(storage, &standing))
            .await
            .expect("atomic explicit registration");
    }

    async fn claim_watch(
        storage: &StorageClient,
        watch: &SourceWatch,
        actor: &str,
    ) -> (PreparedSourceWatchProducer, AttemptFence, RecordId) {
        let id = Box::pin(enqueue_source_watch_refresh(storage, watch, actor, 0))
            .await
            .expect("enqueue watch");
        let item = Box::pin(WorkItemRepository::new(storage.clone()).claim_with_lease(
            &watch.base.space_id,
            &id,
            "watch-worker",
            Utc::now() + Duration::minutes(5),
        ))
        .await
        .expect("claim")
        .expect("claimed");
        let attempt =
            elephant_storage::work_attempt_repository::fence_from_claim(&item).expect("fence");
        let mut base = item.base.clone();
        base.id = RecordId::new_v4(TableName::WorkRun);
        let mut run = elephant_types::records::WorkRun {
            base,
            work_item: Link::new(id),
            attempt: item.attempts,
            started_at: Utc::now(),
            completed_at: None,
            success: false,
            error: None,
            metrics: Default::default(),
            worker_id: "watch-worker".into(),
            worker_version: "fixture".into(),
            model_version: None,
            prompt_id: None,
            config_hash: None,
        };
        run.metrics.custom.insert(
            "attempt_fence".into(),
            serde_json::to_value(&attempt).expect("fence value"),
        );
        let run = Box::pin(elephant_storage::WorkRunRepository::new(storage.clone()).create(run))
            .await
            .expect("run");
        let producer = Box::pin(PreparedSourceWatchProducer::capture(
            storage,
            watch,
            &item.base.created_by,
            &attempt,
            &run.base.id,
        ))
        .await
        .expect("producer");
        (producer, attempt, run.base.id)
    }

    // Inspect every persisted row without invoking the backup API: these
    // oracles deliberately retain a live original lease. The transaction view
    // only scans; it performs no writes or lease transitions.
    async fn live_table_rows(store: &elephant_storage::EmbeddedStore) -> serde_json::Value {
        let names = store
            .persisted_table_names()
            .await
            .expect("persisted tables");
        store
            .mutate(|tx| {
                let mut tables = serde_json::Map::new();
                for name in names {
                    let rows: std::collections::BTreeMap<String, serde_json::Value> =
                        if let Ok(table) = name.parse::<TableName>() {
                            tx.scan_raw_rows(table)?
                                .into_iter()
                                .map(|(id, row)| (id.to_string(), row))
                                .collect()
                        } else {
                            tx.scan_extension_rows(&name)?.into_iter().collect()
                        };
                    tables.insert(name, serde_json::json!(rows));
                }
                Ok(serde_json::json!({"tables": tables}))
            })
            .await
            .expect("inspect live table rows")
    }

    #[expect(
        clippy::unnecessary_get_then_check,
        reason = "Retain the original source-watch frame oracle and exact assertions for attempt and cost-ledger receipt presence."
    )]
    async fn changed_text_contract(sealed: bool, queued: bool, acceptance_gap: bool) {
        let storage = StorageClient::new_embedded()
            .await
            .expect("embedded storage");
        let space_id = "source-watch-test";
        let old_body = b"old body";
        let new_body = "new source material with actual facts";

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind source server");
        let address = listener.local_addr().expect("source server address");
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("accept request");
            let mut request = [0_u8; 2048];
            let _ = socket.read(&mut request).await.expect("read request");
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nETag: \"v2\"\r\nContent-Length: {}\r\n\r\n{}",
                new_body.len(),
                new_body
            );
            socket
                .write_all(response.as_bytes())
                .await
                .expect("write response");
        });

        let source_id = RecordId::new_v4(TableName::Source);
        let mut source = Source::new(
            BaseFields::new(
                source_id.clone(),
                space_id.to_string(),
                "tester".to_string(),
            ),
            SourceType::Web,
            sha256_hex(old_body),
        );
        source.uri = Some(format!("http://{address}/facts.txt"));
        source.external_id.clone_from(&source.uri);
        source.metadata = serde_json::json!({"contact":{"name":"Watched Person","email":"watch@example.invalid"}});
        SpaceConfigRepository::new(storage.clone())
            .create(SpaceConfig {
                base: BaseFields::new(
                    RecordId::new_v4(TableName::SpaceConfig),
                    space_id.into(),
                    "fixture".into(),
                ),
                embedding_provider: "fake".into(),
                embedding_model: "fixture".into(),
                embedding_dims: 8,
                processing: None,
                purpose: None,
                skill: None,
            })
            .await
            .expect("named space");

        let doc_id = RecordId::new_v4(TableName::Doc);
        let mut doc_base = source.base.clone();
        doc_base.id = doc_id.clone();
        let doc = Doc {
            base: doc_base,
            title: "Facts".to_string(),
            text: String::from_utf8(old_body.to_vec()).expect("old UTF-8"),
            sidecar_text: None,
            media_attachments: Vec::new(),
            source: Some(Link::new(source_id.clone())),
            rendition_key: "initial".to_string(),
            metadata: serde_json::json!({"content_type": "text/plain"}),
        };
        source
            .set_manual_doc_template(&elephant_types::records::manual_doc_template_from_doc(&doc))
            .expect("template");
        let prepared = PreparedSourceIntake::prepare(&storage, source, IntakeControl::default())
            .await
            .expect("prepare");
        let original = if sealed {
            let contact = prepared
                .prepare_contact(&storage)
                .await
                .expect("prepare contact")
                .expect("contact");
            prepared
                .accept_with_prepared_contact(&storage, &contact)
                .await
                .expect("sealed acceptance")
        } else {
            prepared.accept(&storage).await.expect("plain acceptance")
        };
        let source = SourceRepository::new(storage.clone())
            .get(space_id, &original.source.base.id)
            .await
            .expect("live source")
            .expect("accepted live source");
        let source_id = source.base.id.clone();
        let original_doc = original.document.expect("initial immutable document");

        let now = Utc::now();
        let mut watch_base = source.base.clone();
        watch_base.id = RecordId::new_v4(TableName::SourceWatch);
        let watch = SourceWatch {
            base: watch_base,
            source: Link::new(source_id.clone()),
            requested_uri: source.uri.clone().expect("source URI"),
            status: SourceWatchStatus::Enabled,
            interval_seconds: 3600,
            next_check_at: now,
            last_checked_at: None,
            last_changed_at: None,
            etag: None,
            last_modified: None,
            observed_hash: Some(source.hash.clone()),
            consecutive_failures: 0,
            last_error: None,
            latest_check: None,
        };
        SourceWatchRepository::new(storage.clone())
            .create(watch.clone())
            .await
            .expect("create watch");

        if queued {
            register_local_watch(&storage, &source, &watch).await;
        }
        let execution = if queued {
            Some(Box::pin(claim_watch(&storage, &watch, "source-watch-test")).await)
        } else {
            None
        };
        let policy = RemoteFetchPolicy {
            allow_private_networks: true,
            ..RemoteFetchPolicy::web_watch()
        };
        let service = if queued {
            SourceWatchService::with_fetch_policy(storage.clone(), policy)
        } else {
            SourceWatchService::with_trusted_local_fetch_policy(
                storage.clone(),
                local_admission(&watch),
                policy,
            )
            .expect("explicit trusted-local constructor")
        };
        if acceptance_gap {
            let (producer, attempt, _) = execution.as_ref().expect("queued gap fixture");
            // Actual acceptance and its initial queue survive before the Watch
            // observation is advanced. A later real claim must not accept G3.
            let staged = Box::pin(service.stage_changed_text_source(
                &original.source,
                "text/plain",
                original.source.uri.as_deref().expect("URI"),
                &sha256_hex(new_body.as_bytes()),
                new_body.into(),
                &watch.base.created_by,
                &original.publication,
                SourceAuthorizationSnapshot::from(&source),
                Some(producer),
            ))
            .await
            .expect("accepted replacement before watch check");
            let child = match staged {
                StagedWatchSource::Staged(Some(id)) => Some(id),
                _ => None,
            }
            .expect("real initial queue");
            let publications = SourcePublicationRepository::new(storage.clone());
            let accepted_head = Box::pin(publications.head(space_id, &source_id))
                .await
                .expect("head")
                .expect("accepted head");
            assert_ne!(accepted_head.generation, original.publication.generation);
            let accepted_receipt =
                Box::pin(publications.operation(space_id, &accepted_head.operation_id))
                    .await
                    .expect("receipt")
                    .expect("accepted receipt");
            assert_eq!(
                accepted_receipt.state,
                elephant_types::publication::OperationState::Accepted
            );
            let queue = WorkItemRepository::new(storage.clone());
            let child_before = Box::pin(queue.get(space_id, &child)).await.expect("child");
            assert!(child_before.is_some());
            Box::pin(queue.update_running_for_worker(
                space_id,
                &attempt.work_item_id,
                &attempt.lease_token,
                WorkItemStatus::Cancelled,
                Some("stopped before recording observation".into()),
            ))
            .await
            .expect("cancel original")
            .expect("original cancellation");
            let (_, next_attempt, next_run) =
                Box::pin(claim_watch(&storage, &watch, "source-watch-test")).await;
            assert_ne!(next_attempt, *attempt);
            let store = storage.embedded_store().expect("store");
            let mut before: serde_json::Value = live_table_rows(&store).await;
            let outcome = Box::pin(service.check_with_attempt(
                watch.clone(),
                &watch.base.created_by,
                &next_attempt,
                &next_run,
            ))
            .await
            .expect("new claim fetches already accepted bytes");
            let observed = match outcome {
                SourceWatchAttemptOutcome::Observed(run) => Some(run),
                SourceWatchAttemptOutcome::ReceiptOnly(_) => None,
            }
            .expect("genuine observation, not a hash-derived operation receipt");
            assert_eq!(observed.check.outcome, SourceWatchOutcome::HashUnchanged);
            assert_eq!(observed.check.prior_hash, watch.observed_hash);
            assert_eq!(
                observed.check.observed_hash,
                Some(sha256_hex(new_body.as_bytes()))
            );
            assert_eq!(observed.watch.observed_hash, observed.check.observed_hash);
            assert!(observed.triage_work_item.is_none());
            assert_eq!(
                Box::pin(publications.head(space_id, &source_id))
                    .await
                    .expect("head"),
                Some(accepted_head)
            );
            assert_eq!(
                Box::pin(publications.operation(space_id, &accepted_receipt.operation_id))
                    .await
                    .expect("receipt"),
                Some(accepted_receipt)
            );
            assert_eq!(
                Box::pin(queue.get(space_id, &child)).await.expect("child"),
                child_before
            );
            let mut after: serde_json::Value = live_table_rows(&store).await;
            // The real observation transaction writes its attempt conflict
            // nonce and the Watch/check records. Those three guarded writes
            // advance the erasure revision; replacing the Watch also rotates
            // the canonical epoch. Assert those exact control effects before
            // comparing every other persisted field, including the queue.
            let epoch = format!("canonical_epoch:{space_id}");
            let erasure = format!("source_erasure_guard:{space_id}");
            let attempt_id = format!("work_attempt:{}", next_attempt.work_item_id.id());
            for (table, id, field) in [
                ("canonical_epoch", epoch.as_str(), "epoch"),
                ("work_attempt", attempt_id.as_str(), "commit_nonce"),
            ] {
                let old = &before["tables"][table][id][field];
                let new = &after["tables"][table][id][field];
                assert_ne!(old, new, "genuine observation must touch {table}.{field}");
                assert!(
                    new.as_str()
                        .is_some_and(|value| uuid::Uuid::parse_str(value).is_ok()),
                    "the new conflict token must be a UUID"
                );
                after["tables"][table][id][field] = old.clone();
            }
            let old_revision = before["tables"]["source_erasure_guard"][&erasure]["revision"]
                .as_u64()
                .expect("original physical erasure revision");
            assert_eq!(
                after["tables"]["source_erasure_guard"][&erasure]["revision"].as_u64(),
                old_revision.checked_add(3),
                "attempt nonce, Watch check and Watch replacement are three guarded writes"
            );
            after["tables"]["source_erasure_guard"][&erasure]["revision"] = old_revision.into();
            for snapshot in [&mut before, &mut after] {
                let tables = snapshot["tables"].as_object_mut().expect("tables");
                tables.remove("source_watch");
                tables.remove("source_watch_check");
            }
            assert_eq!(before["tables"], after["tables"],
                "only the genuine Watch observation and asserted conflict tokens may change; source/head/receipt/docs/queue remain intact");
            return;
        }
        let run = if let Some((_, attempt, run_id)) = &execution {
            let check =
                service.check_with_attempt(watch.clone(), &watch.base.created_by, attempt, run_id);
            assert!(
                std::mem::size_of_val(&check) <= 64 * 1024,
                "queued Watch future must not inline acceptance and observation subgraphs"
            );
            let outcome = Box::pin(check).await.expect("original queued check");
            match outcome {
                SourceWatchAttemptOutcome::Observed(run) => Some(run),
                SourceWatchAttemptOutcome::ReceiptOnly(_) => None,
            }
            .expect("a new changed fetch must create a real check")
        } else {
            let check = service.check(watch.clone(), "source-watch-test");
            assert!(
                std::mem::size_of_val(&check) <= 64 * 1024,
                "direct Watch future must not inline acceptance and observation subgraphs"
            );
            Box::pin(check).await.expect("check source")
        };
        let producer = execution.as_ref().map(|(producer, _, _)| producer);

        assert_eq!(run.check.outcome, SourceWatchOutcome::Changed);
        assert_eq!(run.watch.observed_hash, run.check.observed_hash);
        assert_eq!(run.watch.etag.as_deref(), Some("\"v2\""));
        assert!(run.triage_work_item.is_some());
        let refreshed_source = SourceRepository::new(storage.clone())
            .get(space_id, &source_id)
            .await
            .expect("read source")
            .expect("source");
        assert_eq!(
            refreshed_source.lifecycle_status,
            SourceLifecycleStatus::Discovered
        );
        assert_eq!(refreshed_source.hash, sha256_hex(new_body.as_bytes()));
        let docs = DocRepository::new(storage.clone())
            .find_by_source(space_id, &source_id, 2)
            .await
            .expect("read docs");
        assert_eq!(docs.len(), 2);
        assert!(docs
            .iter()
            .any(|doc| doc.base.id == original_doc.base.id && doc.text == original_doc.text));
        let changed = docs
            .iter()
            .find(|doc| doc.text == new_body)
            .expect("new immutable document");
        assert_ne!(changed.base.id, original_doc.base.id);
        let work = WorkItemRepository::new(storage.clone())
            .get(space_id, &run.triage_work_item.clone().expect("work id"))
            .await
            .expect("read work item")
            .expect("work");
        let head = SourcePublicationRepository::new(storage.clone())
            .head(space_id, &source_id)
            .await
            .expect("head")
            .expect("active head");
        assert_eq!(work.payload["_publication"]["generation"], head.generation);
        let accepted_fence: PublicationFence =
            serde_json::from_value(work.payload["_publication"].clone())
                .expect("original queued fence");
        assert_eq!(
            SourcePublicationRepository::new(storage.clone())
                .accepted_contact_for_publication(space_id, &accepted_fence)
                .await
                .expect("actual capability")
                .is_some(),
            sealed
        );
        assert_ne!(head.generation, original.publication.generation);
        assert!(work.payload.get("_attempt_fence").is_none());
        if !queued {
            let authority =
                elephant_storage::standing_watch_repository::StandingWatchRepository::new(
                    storage.clone(),
                )
                .capture_for_local_scheduling(&run.watch, &local_admission(&watch))
                .await
                .expect("original local registration retained");
            assert_eq!(authority.principal().space_id, space_id);
            let runs = elephant_storage::WorkRunRepository::new(storage.clone());
            let items = storage
                .embedded_store()
                .expect("store")
                .scan::<WorkItem>()
                .await
                .expect("work rows");
            let refresh = items
                .iter()
                .find(|item| item.kind == REFRESH_SOURCE_WATCH_KIND)
                .expect("actual refresh work");
            assert_eq!(refresh.status, WorkItemStatus::Done);
            let actual_runs = runs
                .find_by_work_item(&refresh.base.id)
                .await
                .expect("runs");
            assert_eq!(actual_runs.len(), 1);
            let actual_run = &actual_runs[0];
            assert!(actual_run.success && actual_run.completed_at.is_some());
            assert!(actual_run.metrics.custom.get("attempt_fence").is_some());
            assert!(actual_run.metrics.custom.get("cost_ledger_id").is_some());
            assert_eq!(actual_run.metrics.custom["usage"]["basis"], "not_started");
            assert!(actual_run.metrics.custom["usage"]["measured_tokens"].is_null());
            return;
        }
        let replay = SourceWatchService::new(storage.clone())
            .stage_changed_text_source(
                &original.source,
                "text/plain",
                original.source.uri.as_deref().expect("uri"),
                &sha256_hex(new_body.as_bytes()),
                new_body.into(),
                &watch.base.created_by,
                &original.publication,
                SourceAuthorizationSnapshot::from(&source),
                producer,
            )
            .await
            .expect("original changed-fetch retry");
        if queued {
            let receipt = match replay {
                StagedWatchSource::ReceiptOnly(receipt) => Some(receipt),
                StagedWatchSource::Staged(_) => None,
            }
            .expect("matching original attempt only returns its receipt");
            assert_eq!(receipt.operation_id, accepted_fence.operation_id);
            assert_eq!(
                receipt.state,
                elephant_types::publication::OperationState::Accepted
            );
            let store = storage.embedded_store().expect("store");
            assert!(
                matches!(
                    store.export_snapshot_bytes().await,
                    Err(elephant_storage::StorageError::Conflict(message))
                        if message.contains("still running")
                ),
                "backup export must retain the running-work barrier"
            );
            let before: serde_json::Value = live_table_rows(&store).await;
            let outcome = Box::pin(service.finish_snapshot(
                watch.clone(),
                original.source.clone(),
                RemoteSnapshot {
                    final_url: watch.requested_uri.parse().expect("URL"),
                    content_type: Some("text/plain".into()),
                    validators: RemoteValidators::default(),
                    bytes: new_body.as_bytes().to_vec(),
                },
                Utc::now(),
                &watch.base.created_by,
                original.publication.clone(),
                SourceAuthorizationSnapshot::from(&source),
                producer,
                None,
            ))
            .await
            .expect("outer original retry");
            assert!(
                matches!(outcome, SourceWatchAttemptOutcome::ReceiptOnly(ref original) if original == &receipt)
            );
            let after: serde_json::Value = live_table_rows(&store).await;
            assert_eq!(
                before["tables"], after["tables"],
                "receipt-only retry cannot write a check, document, operation or queue row"
            );
        } else {
            assert_eq!(
                replay,
                StagedWatchSource::Staged(Some(work.base.id.clone()))
            );
        }
        if sealed {
            let store = storage.embedded_store().expect("native store");
            store
                .mutate(|tx| {
                    let matching: Vec<_> = tx
                        .scan_extension_rows("source_contact_intent")?
                        .into_iter()
                        .filter(|(_, row)| {
                            let payload: serde_json::Value = serde_json::from_str(
                                row["payload"].as_str().expect("private payload"),
                            )
                            .expect("private intent");
                            payload["publication"]["operation_id"] == accepted_fence.operation_id
                        })
                        .collect();
                    assert_eq!(matching.len(), 1);
                    tx.hard_delete_extension_row("source_contact_intent", &matching[0].0, space_id)
                        .map_err(Into::into)
                })
                .await
                .expect("original watch plan loss fault");
            let before: serde_json::Value = live_table_rows(&store).await;
            let unavailable = SourceWatchService::new(storage.clone())
                .stage_changed_text_source(
                    &original.source,
                    "text/plain",
                    original.source.uri.as_deref().expect("uri"),
                    &sha256_hex(new_body.as_bytes()),
                    new_body.into(),
                    &watch.base.created_by,
                    &original.publication,
                    SourceAuthorizationSnapshot::from(&source),
                    producer,
                )
                .await;
            assert!(matches!(unavailable,
                Err(PipelineError::InvalidState(message))
                    if message == "accepted_contact_plan_unavailable"));
            if producer.is_some() {
                let result = Box::pin(service.finish_snapshot(
                    watch.clone(),
                    original.source.clone(),
                    RemoteSnapshot {
                        final_url: watch.requested_uri.parse().expect("URL"),
                        content_type: Some("text/plain".into()),
                        validators: RemoteValidators::default(),
                        bytes: new_body.as_bytes().to_vec(),
                    },
                    Utc::now(),
                    &watch.base.created_by,
                    original.publication.clone(),
                    SourceAuthorizationSnapshot::from(&source),
                    producer,
                    None,
                ))
                .await;
                assert!(
                    matches!(result, Err(PipelineError::InvalidState(ref category))
                    if category == "accepted_contact_plan_unavailable"),
                    "outer queued boundary must not convert refusal into a Failed check"
                );
            }
            let after: serde_json::Value = live_table_rows(&store).await;
            assert_eq!(before["tables"], after["tables"]);
        }
        // A fetch started under the prior generation cannot claim this head.
        let stale = SourceWatchService::new(storage.clone())
            .stage_changed_text_source(
                &original.source,
                "text/plain",
                original.source.uri.as_deref().expect("uri"),
                &sha256_hex(b"stale fetched bytes"),
                "stale fetched bytes".into(),
                &watch.base.created_by,
                &original.publication,
                SourceAuthorizationSnapshot::from(&source),
                producer,
            )
            .await;
        assert!(stale.is_err());
        let mut restricted = changed.clone();
        restricted.base.security.level += 1;
        DocRepository::new(storage.clone())
            .update(space_id, &restricted.base.id.clone(), restricted)
            .await
            .expect("independent document reclassification");
        let protected = SourceWatchService::new(storage.clone())
            .stage_changed_text_source(
                &refreshed_source,
                "text/plain",
                refreshed_source.uri.as_deref().expect("uri"),
                "protected-update",
                "new bytes".into(),
                &watch.base.created_by,
                &accepted_fence,
                SourceAuthorizationSnapshot::from(&refreshed_source),
                producer,
            )
            .await;
        assert!(
            protected.is_err(),
            "saved source authority cannot bypass a stricter physical Doc"
        );
        assert_eq!(
            SourcePublicationRepository::new(storage)
                .head(space_id, &source_id)
                .await
                .expect("head")
                .expect("head")
                .generation,
            head.generation
        );
    }

    #[tokio::test]
    async fn changed_text_is_staged_receipted_and_enqueued() {
        Box::pin(changed_text_contract(false, false, false)).await;
    }

    #[tokio::test]
    async fn changed_text_preserves_original_admitted_contact_capability() {
        Box::pin(changed_text_contract(true, false, false)).await;
    }

    #[tokio::test]
    async fn queued_watch_retries_return_original_receipt_without_new_observation() {
        Box::pin(changed_text_contract(false, true, false)).await;
    }

    #[tokio::test]
    async fn queued_contact_watch_unavailable_plan_is_terminal_without_check_mutation() {
        Box::pin(changed_text_contract(true, true, false)).await;
    }

    #[tokio::test]
    async fn new_watch_claim_observes_accepted_bytes_without_superseding_pending_head() {
        Box::pin(changed_text_contract(false, true, true)).await;
    }

    #[tokio::test]
    async fn refresh_work_is_idempotent_and_reopens_terminal_item() {
        let storage = StorageClient::new_embedded()
            .await
            .expect("embedded storage");
        let source_id = RecordId::new_v4(TableName::Source);
        let mut base = BaseFields::new(
            RecordId::new_v4(TableName::SourceWatch),
            "watch-queue-space".to_string(),
            "tester".to_string(),
        );
        base.security.level = 3;
        let watch = SourceWatch {
            base,
            source: Link::new(source_id),
            requested_uri: "https://example.com/facts.txt".to_string(),
            status: SourceWatchStatus::Enabled,
            interval_seconds: 3600,
            next_check_at: Utc::now(),
            last_checked_at: None,
            last_changed_at: None,
            etag: None,
            last_modified: None,
            observed_hash: Some("old-hash".to_string()),
            consecutive_failures: 0,
            last_error: None,
            latest_check: None,
        };
        SpaceConfigRepository::new(storage.clone())
            .create(SpaceConfig {
                base: BaseFields::new(
                    RecordId::new_v4(TableName::SpaceConfig),
                    "watch-queue-space".into(),
                    "fixture".into(),
                ),
                embedding_provider: "fake".into(),
                embedding_model: "fixture".into(),
                embedding_dims: 8,
                processing: None,
                purpose: None,
                skill: None,
            })
            .await
            .expect("named space");
        let mut source_base = watch.base.clone();
        source_base.id = watch.source.id().clone();
        let mut source = Source::new(source_base, SourceType::Web, "old-hash".into());
        source.uri = Some(watch.requested_uri.clone());
        source
            .set_manual_doc_template(&elephant_types::records::ManualDocTemplate {
                doc_id: RecordId::new_v4(TableName::Doc).to_string(),
                title: "Watched".into(),
                text: "Original queue input".into(),
                sidecar_text: None,
                media_attachments: Vec::new(),
                rendition_key: "watch-input".into(),
                metadata: serde_json::json!({}),
            })
            .expect("template");
        let prepared = Box::pin(PreparedSourceIntake::prepare(
            &storage,
            source,
            IntakeControl::default(),
        ))
        .await
        .expect("prepare input");
        let source = Box::pin(prepared.accept(&storage))
            .await
            .expect("real input acceptance")
            .source;
        let source = SourceRepository::new(storage.clone())
            .get("watch-queue-space", &source.base.id)
            .await
            .expect("live source")
            .expect("source");
        let mut watch = watch;
        watch.source = Link::new(source.base.id.clone());
        SourceWatchRepository::new(storage.clone())
            .create(watch.clone())
            .await
            .expect("watch");
        register_local_watch(&storage, &source, &watch).await;
        let first = enqueue_source_watch_refresh(&storage, &watch, "scheduler", 5)
            .await
            .expect("enqueue first");
        let second = enqueue_source_watch_refresh(&storage, &watch, "scheduler", 5)
            .await
            .expect("reuse runnable");
        assert_eq!(first, second);

        let repository = WorkItemRepository::new(storage.clone());
        let mut item = repository
            .get("watch-queue-space", &first)
            .await
            .expect("load item")
            .expect("item exists");
        item.status = WorkItemStatus::Done;
        item.attempts = 4;
        repository
            .update("watch-queue-space", &first, item)
            .await
            .expect("mark terminal");
        let reopened = enqueue_source_watch_refresh(&storage, &watch, "scheduler", 9)
            .await
            .expect("reopen terminal");
        assert_eq!(reopened, first);
        let item = repository
            .get("watch-queue-space", &first)
            .await
            .expect("load reopened item")
            .expect("reopened item exists");
        assert_eq!(item.status, WorkItemStatus::Runnable);
        assert_eq!(item.priority, 9);
        assert_eq!(item.attempts, 0);
        assert_eq!(item.base.security.level, 3);
        let mut observed = watch.clone();
        observed.observed_hash = Some("new-observation".into());
        observed.next_check_at = Utc::now() + Duration::hours(1);
        SourceWatchRepository::new(storage.clone())
            .update(
                "watch-queue-space",
                &observed.base.id.clone(),
                observed.clone(),
            )
            .await
            .expect("persist next scheduling slot");
        let next_slot = enqueue_source_watch_refresh(&storage, &observed, "scheduler", 9)
            .await
            .expect("new durable slot");
        assert_ne!(next_slot, first);
        let mut policy = observed.clone();
        policy.base.security.handling.push("new-policy".into());
        let records = Box::pin(
            elephant_storage::source_watch_registration::PreparedSourceWatchRegistration::prepare(
                &storage,
                &source,
                Some(&observed),
                policy.clone(),
            ),
        )
        .await
        .expect("original policy records");
        let authority = elephant_storage::standing_watch_repository::StandingWatchRepository::new(
            storage.clone(),
        )
        .prepare_registration(&policy, &local_admission(&policy))
        .await
        .expect("explicit new delegation");
        Box::pin(records.register(&storage, &authority))
            .await
            .expect("atomic authorized reconfiguration");
        let new_policy = enqueue_source_watch_refresh(&storage, &policy, "scheduler", 9)
            .await
            .expect("new policy intent");
        assert_ne!(new_policy, first);
        assert_ne!(new_policy, next_slot);
        let same_actor_independent_intent =
            enqueue_source_watch_refresh(&storage, &policy, "operator", 9)
                .await
                .expect("actor is not authority");
        assert_eq!(same_actor_independent_intent, new_policy);
        assert_eq!(
            repository
                .get("watch-queue-space", &first)
                .await
                .expect("original")
                .expect("original retained"),
            item
        );
        let policy_item = repository
            .get("watch-queue-space", &new_policy)
            .await
            .expect("policy")
            .expect("policy item");
        assert_eq!(policy_item.base.security, policy.base.security);
        assert_eq!(
            policy_item.payload["watch_intent_fingerprint"],
            elephant_storage::source_watch_producer::watch_intent_fingerprint(
                &policy,
                &policy.base.created_by
            )
            .expect("intent")
        );
    }
}
