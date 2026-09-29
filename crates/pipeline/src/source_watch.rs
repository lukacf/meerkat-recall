//! Execution service for durable remote source watches.

use chrono::{DateTime, Duration, Utc};
use elephant_storage::{
    persist_manual_doc_for_source_detailed, rollback_persisted_manual_doc,
    update_existing_source_for_reingest, DocRepository, KnowledgeViewRepository, Repository,
    SourceRepository, SourceWatchRepository, StorageClient, WorkItemRepository,
};
use elephant_types::records::{
    apply_manual_doc_to_source, prepare_source_for_reingest, Source, SourceType, SourceWatch,
    SourceWatchCheck, SourceWatchOutcome, SourceWatchStatus, WorkItem, WorkItemStatus,
};
use elephant_types::{Link, RecordId, TableName};
use sha2::{Digest, Sha256};

use crate::error::{PipelineError, PipelineResult};
use crate::remote_source::{
    fetch_remote_source, RemoteFetchOutcome, RemoteFetchPolicy, RemoteSnapshot, RemoteValidators,
};
use crate::scheduler::ingest_work_kinds::enqueue_triage_source_work_item;

const MAX_RECORDED_ERROR_BYTES: usize = 2048;
const MAX_RETRY_SECONDS: i64 = 24 * 60 * 60;

/// Durable work kind that executes one source-watch observation.
pub const REFRESH_SOURCE_WATCH_KIND: &str = "refresh_source_watch";

/// Enqueues or reopens the single durable refresh item for a watch.
///
/// The watch's own `next_check_at` controls cadence. Reusing one work item
/// avoids an unbounded history of empty scheduler rows; immutable observation
/// history is retained in `source_watch_check` records.
pub async fn enqueue_source_watch_refresh(
    storage: &StorageClient,
    watch: &SourceWatch,
    actor: &str,
    priority: i32,
) -> PipelineResult<RecordId> {
    validate_watch(watch)?;
    let repository = WorkItemRepository::new(storage.clone());
    let idempotency_key = format!("{REFRESH_SOURCE_WATCH_KIND}:{}", watch.base.id);
    if let Some(mut existing) = repository
        .find_by_idempotency_key(&watch.base.space_id, &idempotency_key)
        .await?
    {
        if matches!(
            existing.status,
            WorkItemStatus::Done | WorkItemStatus::Failed | WorkItemStatus::Cancelled
        ) {
            let id = existing.base.id.clone();
            existing.base.security = watch.base.security.clone();
            existing.base.provenance = watch.base.provenance.clone();
            existing.base.subjects.clone_from(&watch.base.subjects);
            existing.base.subjects_state = watch.base.subjects_state;
            existing.base.updated_at = Utc::now();
            existing.status = WorkItemStatus::Runnable;
            existing.priority = priority;
            existing.attempts = 0;
            existing.lease_expires_at = None;
            existing.leased_by = None;
            existing.run_after = None;
            existing.last_error = None;
            return repository
                .update(&watch.base.space_id, &id, existing)
                .await
                .map(|item| item.base.id)
                .map_err(Into::into);
        }
        return Ok(existing.base.id);
    }

    let id = RecordId::new(
        TableName::WorkItem,
        uuid::Uuid::new_v5(
            &uuid::Uuid::NAMESPACE_OID,
            format!("{}|{idempotency_key}", watch.base.space_id).as_bytes(),
        ),
    );
    let mut base = watch.base.clone();
    base.id = id.clone();
    base.created_at = Utc::now();
    base.updated_at = base.created_at;
    base.created_by = actor.to_string();
    base.deleted_at = None;
    let item = WorkItem {
        base,
        kind: REFRESH_SOURCE_WATCH_KIND.to_string(),
        payload: serde_json::json!({"watch_id": watch.base.id.to_string()}),
        status: WorkItemStatus::Runnable,
        priority,
        idempotency_key,
        attempts: 0,
        lease_expires_at: None,
        leased_by: None,
        run_after: None,
        last_error: None,
    };
    match repository.create(item).await {
        Ok(_) => Ok(id),
        Err(elephant_storage::StorageError::Conflict(_)) => repository
            .find_by_idempotency_key(
                &watch.base.space_id,
                &format!("{REFRESH_SOURCE_WATCH_KIND}:{}", watch.base.id),
            )
            .await?
            .map(|item| item.base.id)
            .ok_or_else(|| {
                PipelineError::InvalidState(
                    "source-watch refresh conflict without existing work item".to_string(),
                )
            }),
        Err(error) => Err(error.into()),
    }
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

/// Executes recurring remote observations and source-first re-ingest staging.
#[derive(Clone, Debug)]
pub struct SourceWatchService {
    storage: StorageClient,
    watches: SourceWatchRepository,
    sources: SourceRepository,
    docs: DocRepository,
    fetch_policy: RemoteFetchPolicy,
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
            docs: DocRepository::new(storage.clone()),
            storage,
            fetch_policy,
        }
    }

    /// Checks a watch now and always attempts to persist a conclusive receipt.
    ///
    /// Remote failures are represented as successful `Failed` receipts. Errors
    /// are returned only when Elephant cannot load or durably record state, or
    /// when changed content cannot be staged safely.
    pub async fn check(&self, watch: SourceWatch, actor: &str) -> PipelineResult<SourceWatchRun> {
        validate_watch(&watch)?;
        let started_at = Utc::now();
        let source = self
            .sources
            .get(&watch.base.space_id, watch.source.id())
            .await?
            .ok_or_else(|| PipelineError::NotFound(watch.source.id().to_string()))?;
        validate_security_envelope(&watch, &source)?;

        if source.type_ != SourceType::Web {
            return self
                .finish_unsupported(
                    watch,
                    started_at,
                    format!("source type {:?} is not watchable", source.type_),
                )
                .await;
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
                self.finish_success(watch, check, None).await
            }
            Ok(RemoteFetchOutcome::Snapshot(snapshot)) => {
                self.finish_snapshot(watch, source, snapshot, started_at, actor)
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
                self.finish_failure(watch, check, message).await
            }
        }
    }

    async fn finish_snapshot(
        &self,
        watch: SourceWatch,
        source: Source,
        snapshot: RemoteSnapshot,
        started_at: DateTime<Utc>,
        actor: &str,
    ) -> PipelineResult<SourceWatchRun> {
        let completed_at = Utc::now();
        let observed_hash = sha256_hex(&snapshot.bytes);
        let prior_hash = watch
            .observed_hash
            .clone()
            .or_else(|| Some(source.hash.clone()));
        let content_length = u64::try_from(snapshot.bytes.len()).map_err(|_| {
            PipelineError::InvalidInput("remote source content length exceeds u64".to_string())
        })?;

        if prior_hash.as_deref() == Some(observed_hash.as_str()) {
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
            return self.finish_success(watch, check, None).await;
        }

        let Some(content_type) = snapshot.content_type.clone() else {
            return self
                .finish_unsupported(
                    watch,
                    started_at,
                    "remote source omitted Content-Type".to_string(),
                )
                .await;
        };
        if !is_supported_text_type(&content_type) {
            return self
                .finish_unsupported(
                    watch,
                    started_at,
                    format!("remote media type {content_type} is not yet watchable"),
                )
                .await;
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
                return self.finish_failure(watch, check, message).await;
            }
        };

        let staged = self
            .stage_changed_text_source(
                &source,
                &content_type,
                snapshot.final_url.as_str(),
                &observed_hash,
                text,
                actor,
            )
            .await;
        let triage_work_item = match staged {
            Ok(work_item) => work_item,
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
                return self.finish_failure(watch, check, message).await;
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
        self.finish_success(watch, check, Some(triage_work_item))
            .await
    }

    async fn stage_changed_text_source(
        &self,
        source: &Source,
        content_type: &str,
        final_uri: &str,
        hash: &str,
        text: String,
        actor: &str,
    ) -> PipelineResult<RecordId> {
        let mut docs = self
            .docs
            .find_by_source(&source.base.space_id, &source.base.id, 2)
            .await?;
        if docs.len() != 1 {
            return Err(PipelineError::InvalidState(format!(
                "watched source {} must have exactly one live document, found {}",
                source.base.id,
                docs.len()
            )));
        }
        let mut doc = docs.pop().ok_or_else(|| {
            PipelineError::InvalidState("source document disappeared".to_string())
        })?;
        doc.text = text;
        doc.sidecar_text = None;
        doc.media_attachments.clear();
        doc.rendition_key = format!("source-watch-{hash}");
        doc.base.updated_at = Utc::now();
        let metadata = doc.metadata.as_object_mut().ok_or_else(|| {
            PipelineError::InvalidState("watched document metadata must be an object".to_string())
        })?;
        metadata.insert(
            "content_type".to_string(),
            serde_json::Value::String(content_type.to_string()),
        );
        metadata.insert(
            "source_type".to_string(),
            serde_json::Value::String(content_type.to_string()),
        );
        metadata.insert(
            "final_url".to_string(),
            serde_json::Value::String(final_uri.to_string()),
        );
        metadata.insert(
            "source_watch_observed_hash".to_string(),
            serde_json::Value::String(hash.to_string()),
        );

        let mut incoming = source.clone();
        incoming.hash = hash.to_string();
        incoming.base.updated_at = doc.base.updated_at;
        apply_manual_doc_to_source(&mut incoming, &doc)
            .map_err(|error| PipelineError::IngestionError(error.to_string()))?;
        let prepared = prepare_source_for_reingest(source, incoming);
        let updated_source =
            update_existing_source_for_reingest(&self.sources, source, prepared).await?;

        let persisted_doc =
            match persist_manual_doc_for_source_detailed(&self.storage, &updated_source, doc).await
            {
                Ok(persisted) => persisted,
                Err(error) => {
                    self.sources
                        .replace_existing_snapshot(
                            &source.base.space_id,
                            &source.base.id,
                            source.clone(),
                        )
                        .await?;
                    return Err(error.into());
                }
            };

        if let Err(error) = KnowledgeViewRepository::new(self.storage.clone())
            .mark_dirty_for_dependencies(
                &source.base.space_id,
                &[
                    source.base.id.to_string(),
                    persisted_doc.doc.base.id.to_string(),
                ],
                "watched source content changed",
            )
            .await
        {
            rollback_persisted_manual_doc(&self.storage, &persisted_doc).await?;
            self.sources
                .replace_existing_snapshot(&source.base.space_id, &source.base.id, source.clone())
                .await?;
            return Err(error.into());
        }

        match enqueue_triage_source_work_item(&self.storage, &updated_source, actor, 5).await {
            Ok(work_item) => Ok(work_item),
            Err(error) => {
                rollback_persisted_manual_doc(&self.storage, &persisted_doc).await?;
                self.sources
                    .replace_existing_snapshot(
                        &source.base.space_id,
                        &source.base.id,
                        source.clone(),
                    )
                    .await?;
                Err(error)
            }
        }
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

    async fn finish_success(
        &self,
        mut watch: SourceWatch,
        check: SourceWatchCheck,
        triage_work_item: Option<RecordId>,
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
        let (watch, check) = self.watches.record_check(watch, check).await?;
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
        let (watch, check) = self.watches.record_check(watch, check).await?;
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
        let (watch, check) = self.watches.record_check(watch, check).await?;
        Ok(SourceWatchRun {
            watch,
            check,
            triage_work_item: None,
        })
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
    use elephant_types::records::base::BaseFields;
    use elephant_types::records::{Doc, SourceLifecycleStatus};
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

    #[tokio::test]
    async fn changed_text_is_staged_receipted_and_enqueued() {
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
        SourceRepository::new(storage.clone())
            .create(source.clone())
            .await
            .expect("create source");

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
        DocRepository::new(storage.clone())
            .create(doc)
            .await
            .expect("create doc");

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

        let run = SourceWatchService::with_fetch_policy(
            storage.clone(),
            RemoteFetchPolicy {
                allow_private_networks: true,
                ..RemoteFetchPolicy::web_watch()
            },
        )
        .check(watch, "source-watch-test")
        .await
        .expect("check source");

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
        assert_eq!(docs.len(), 1);
        assert_eq!(docs[0].text, new_body);
        let work = WorkItemRepository::new(storage)
            .find_by_idempotency_key(space_id, &format!("triage_source:{source_id}"))
            .await
            .expect("read work item");
        assert!(work.is_some());
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
        let reopened = enqueue_source_watch_refresh(&storage, &watch, "operator", 9)
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
    }
}
