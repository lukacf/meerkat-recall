//! Original watch execution authority for source acceptance and observation writes.
//!
//! A historical producer descriptor restricts later accounting; only this prepared guard
//! can certify the original live attempt for a new write.

use std::collections::BTreeMap;

use elephant_types::publication::{AttemptFence, OperationReceipt, SourceProducerOrigin};
use elephant_types::records::{SourceWatch, SourceWatchStatus, WorkItem, WorkItemStatus, WorkRun};
use elephant_types::{RecordId, TableName};
use surrealdb::sql::Value;

use crate::canonical_commit::{source_support_fingerprint, CanonicalRecord};
use crate::source_publication::{
    conflict, integrity, AcceptSourceRevision, SourcePublicationRepository,
};
use crate::standing_watch_repository::PreparedStandingWatchAuthority;
use crate::{EmbeddedMutation, Repository, StorageClient, StorageResult};

/// Actual source acceptance outcome; a receipt-only retry has no enqueue authority.
pub use crate::source_publication::SourceAcceptanceOutcome as WatchSourceAcceptance;

/// Stable requested watch intent, excluding scheduling/observation bookkeeping.
/// Queue creation stores this in its immutable payload before any worker claim.
///
/// # Errors
/// Rejects malformed watch/source identity or an empty service actor.
pub fn watch_intent_fingerprint(watch: &SourceWatch, actor: &str) -> StorageResult<String> {
    if watch.base.id.table() != TableName::SourceWatch
        || watch.source.id().table() != TableName::Source
        || watch.base.space_id.trim().is_empty()
        || actor.trim().is_empty()
    {
        return Err(integrity("invalid original watch intent"));
    }
    source_support_fingerprint(&serde_json::json!({
        "version":1,"space_id":watch.base.space_id,"watch_id":watch.base.id,"source_id":watch.source.id(),
        "requested_uri":watch.requested_uri,"interval_seconds":watch.interval_seconds,
        "security":watch.base.security,"subjects":watch.base.subjects,"subjects_state":watch.base.subjects_state,
        "provenance":watch.base.provenance,"actor":actor,
    }))
}

/// Exact watch service observations taken before the network request. Fields are
/// private so an HTTP/MCP caller cannot mint an accounting or lease capability.
#[derive(Debug, Clone)]
pub struct PreparedSourceWatchProducer {
    watch: SourceWatch,
    work: WorkItem,
    run: WorkRun,
    original_attempt: AttemptFence,
    origin: SourceProducerOrigin,
    standing_authority: PreparedStandingWatchAuthority,
}

impl PreparedSourceWatchProducer {
    /// Captures the actual original watch and claimed execution before fetching.
    /// The mutation transaction checks these again without adopting a new claim.
    ///
    /// # Errors
    /// Rejects a changed watch, wrong service actor/intent/run, or expired claim.
    pub async fn capture(
        client: &StorageClient,
        watch: &SourceWatch,
        actor: &str,
        original_attempt: &AttemptFence,
        original_run_id: &RecordId,
    ) -> StorageResult<Self> {
        let space = &watch.base.space_id;
        let actual = crate::SourceWatchRepository::new(client.clone())
            .get(space, &watch.base.id)
            .await?;
        if actual.as_ref() != Some(watch)
            || watch.base.deleted_at.is_some()
            || !matches!(
                watch.status,
                SourceWatchStatus::Enabled | SourceWatchStatus::Error
            )
            || original_attempt.space_id != *space
            || original_run_id.table() != TableName::WorkRun
            || actor.trim().is_empty()
        {
            return Err(conflict("watch_producer_authority_changed"));
        }
        let work = crate::WorkItemRepository::new(client.clone())
            .get(space, &original_attempt.work_item_id)
            .await?
            .ok_or_else(|| conflict("watch_producer_work_missing"))?;
        let run = crate::WorkRunRepository::new(client.clone())
            .get(space, original_run_id)
            .await?
            .ok_or_else(|| conflict("watch_producer_run_missing"))?;
        if work.base.created_by != actor
            || work.status != WorkItemStatus::Running
            || work
                .lease_expires_at
                .is_none_or(|expires| expires <= chrono::Utc::now())
            || crate::work_attempt_repository::fence_from_claim(&work)? != *original_attempt
            || run.completed_at.is_some()
        {
            return Err(conflict("stale_attempt: watch original execution changed"));
        }
        let intent = watch_intent_fingerprint(watch, actor)?;
        if work
            .payload
            .get("watch_intent_fingerprint")
            .and_then(serde_json::Value::as_str)
            != Some(intent.as_str())
            || work.base.security != watch.base.security
            || work.base.subjects != watch.base.subjects
            || work.base.subjects_state != watch.base.subjects_state
            || work.base.provenance != watch.base.provenance
        {
            return Err(conflict("watch_producer_queued_intent_changed"));
        }
        let standing_authority =
            PreparedStandingWatchAuthority::capture_for_original_watch(client, watch, &work, &run)
                .await?;
        let origin = SourceProducerOrigin::Watch {
            watch_id: watch.base.id.clone(),
            watch_fingerprint: source_support_fingerprint(watch)?,
            work_item_id: work.base.id.clone(),
            work_run_id: run.base.id.clone(),
            original_attempt: original_attempt.clone(),
            work_intent_fingerprint: crate::work_producer_repository::work_intent_fingerprint(
                &work,
            )?,
        };
        verify_watch_origin(&origin, &work, &run)?;
        Ok(Self {
            watch: watch.clone(),
            work,
            run,
            original_attempt: original_attempt.clone(),
            origin,
            standing_authority,
        })
    }

    /// Original observed watch, used to scope an atomic observation write.
    #[must_use]
    pub const fn watch_id(&self) -> &RecordId {
        &self.watch.base.id
    }

    /// Exact space in which the original watch attempt was admitted.
    #[must_use]
    pub fn space_id(&self) -> &str {
        &self.watch.base.space_id
    }

    /// Original standing delegation attenuated by the actual current grant.
    ///
    /// Registration and original queue identity, not an actor string, supply
    /// this principal. Callers still authorize every affected Source and Doc.
    #[must_use]
    pub const fn principal(&self) -> &elephant_policy::Principal {
        self.standing_authority.principal()
    }

    pub(crate) const fn original_work(&self) -> &WorkItem {
        &self.work
    }
    pub(crate) const fn original_run(&self) -> &WorkRun {
        &self.run
    }

    pub(crate) const fn original_watch(&self) -> &SourceWatch {
        &self.watch
    }

    pub(crate) fn origin(&self) -> SourceProducerOrigin {
        self.origin.clone()
    }

    pub(crate) fn validate_request(&self, request: &AcceptSourceRevision) -> StorageResult<()> {
        if request.source.base.space_id != self.watch.base.space_id
            || request.source.base.id != *self.watch.source.id()
            || request.authorized.is_none()
            || request.reactivate
            || request.recreate_after_erasure
        {
            return Err(conflict("watch_producer_source_intent_changed"));
        }
        Ok(())
    }

    pub(crate) fn check_tx(&self, tx: &mut EmbeddedMutation<'_>) -> StorageResult<()> {
        self.standing_authority.check_tx(tx)?;
        crate::work_attempt_repository::check_attempt_fence(
            tx,
            &self.original_attempt,
            chrono::Utc::now(),
        )?;
        let actual = tx.get::<SourceWatch>(self.space_id(), self.watch_id(), false)?;
        let work = tx
            .get::<WorkItem>(self.space_id(), &self.work.base.id, false)?
            .ok_or_else(|| conflict("watch_producer_work_missing"))?;
        let run = tx.get::<WorkRun>(self.space_id(), &self.run.base.id, false)?;
        if actual.as_ref() != Some(&self.watch)
            || run.as_ref() != Some(&self.run)
            || crate::work_producer_repository::work_intent_fingerprint(&work)?
                != crate::work_producer_repository::work_intent_fingerprint(&self.work)?
            || work.payload != self.work.payload
        {
            return Err(conflict("watch_producer_authority_changed"));
        }
        Ok(())
    }

    pub(crate) fn append_guard_sql(
        &self,
        sql: &mut String,
        bindings: &mut BTreeMap<String, Value>,
    ) -> StorageResult<()> {
        if bindings
            .get("space")
            .is_some_and(|value| value != &Value::from(self.space_id()))
        {
            return Err(integrity("watch producer transaction space mismatch"));
        }
        bindings.insert("space".into(), self.space_id().into());
        self.standing_authority.append_guard_sql(sql, bindings)?;
        crate::work_attempt_repository::append_attempt_fence(
            sql,
            bindings,
            &self.original_attempt,
        )?;
        bindings.extend(crate::bindings! {
            "watch_producer_watch"=>crate::surreal::thing_from_record_id(self.watch_id()),
            "watch_producer_space"=>self.space_id(),
            "watch_producer_snapshot"=>crate::canonical_commit::native_expected_fields(crate::SourceWatchRepository::watch_to_value(&self.watch)?),
            "watch_producer_work"=>crate::surreal::thing_from_record_id(&self.work.base.id),
            "watch_producer_work_snapshot"=>work_admission_projection(&self.work)?,
        });
        sql.push_str("LET $watch_producer_current = SELECT * FROM ONLY $watch_producer_watch;\nIF $watch_producer_current = NONE OR $watch_producer_current.space_id != $watch_producer_space { THROW 'publication:watch_producer_authority_changed'; };\nFOR $entry IN object::entries($watch_producer_snapshot) { IF $watch_producer_current[$entry[0]] != $entry[1] { THROW 'publication:watch_producer_authority_changed'; }; };\nLET $watch_producer_work_current = SELECT * FROM ONLY $watch_producer_work;\nIF $watch_producer_work_current = NONE { THROW 'publication:watch_producer_work_missing'; };\nFOR $entry IN object::entries($watch_producer_work_snapshot) { IF $watch_producer_work_current[$entry[0]] != $entry[1] { THROW 'publication:watch_producer_authority_changed'; }; };\nUPDATE $watch_producer_work SET canonical_read_nonce = <string>rand::uuid();\n");
        crate::canonical_commit::append_row_guard(
            sql,
            bindings,
            "watch_producer_run",
            &CanonicalRecord::WorkRun(Box::new(self.run.clone())),
            Some(&CanonicalRecord::WorkRun(Box::new(self.run.clone()))),
        )?;
        // No new schema field: write the already observed value to establish an
        // optimistic conflict with a concurrent watch policy/disable/delete.
        // Source acceptance appends this AFTER its contact epoch guard.
        sql.push_str("UPDATE $watch_producer_watch SET latest_check = $watch_producer_current.latest_check;\n");
        Ok(())
    }
}

/// Preserve lease-renewal compatibility while guarding every immutable queue
/// field used by B's intent fingerprint and the original private attempt payload.
fn work_admission_projection(work: &WorkItem) -> StorageResult<Value> {
    let Value::Object(mut fields) = crate::WorkItemRepository::work_item_to_value(work)? else {
        return Err(integrity(
            "watch work storage representation is not an object",
        ));
    };
    fields.0.retain(|key, _| {
        matches!(
            key.as_str(),
            "space_id"
                | "created_by"
                | "deleted_at"
                | "security"
                | "subjects"
                | "subjects_state"
                | "provenance"
                | "kind"
                | "idempotency_key"
                | "payload"
        )
    });
    Ok(crate::canonical_commit::native_expected_fields(
        Value::Object(fields),
    ))
}

/// Verifies historical accounting identity against the actual hot/archive work and run.
///
/// This grants no live lease and does not classify Watch as unrestricted. The caller must
/// continue B's bounded private producer-chain budget traversal.
///
/// # Errors
/// Rejects missing/wrong original execution, changed work intent, or wrong watch.
pub fn verify_watch_origin(
    origin: &SourceProducerOrigin,
    work: &WorkItem,
    run: &WorkRun,
) -> StorageResult<()> {
    let SourceProducerOrigin::Watch {
        watch_id,
        watch_fingerprint,
        work_item_id,
        work_run_id,
        original_attempt,
        work_intent_fingerprint,
    } = origin
    else {
        return Err(conflict("watch_producer_origin_changed"));
    };
    let captured: AttemptFence = serde_json::from_value(
        run.metrics
            .custom
            .get("attempt_fence")
            .cloned()
            .ok_or_else(|| conflict("watch_producer_original_attempt_missing"))?,
    )
    .map_err(|error| integrity(&error.to_string()))?;
    let actual_watch: RecordId = serde_json::from_value(
        work.payload
            .get("watch_id")
            .cloned()
            .ok_or_else(|| conflict("watch_producer_intent_missing"))?,
    )
    .map_err(|error| integrity(&error.to_string()))?;
    if watch_id.table() != TableName::SourceWatch
        || watch_fingerprint.is_empty()
        || work.kind != "refresh_source_watch"
        || actual_watch != *watch_id
        || work.base.deleted_at.is_some()
        || run.base.deleted_at.is_some()
        || work.base.id != *work_item_id
        || run.base.id != *work_run_id
        || run.work_item.id() != work_item_id
        || original_attempt.work_item_id != *work_item_id
        || work.base.space_id != run.base.space_id
        || original_attempt.space_id != work.base.space_id
        || captured != *original_attempt
        || crate::work_producer_repository::work_intent_fingerprint(work)?
            != *work_intent_fingerprint
    {
        return Err(conflict("watch_producer_origin_changed"));
    }
    Ok(())
}

impl SourcePublicationRepository {
    /// Returns whether this exact transaction created an acceptance or only
    /// replayed its original receipt.
    ///
    /// This classification cannot be inferred from
    /// an earlier lookup because another matching request can win before commit.
    pub async fn accept_with_watch_producer_outcome(
        &self,
        request: AcceptSourceRevision,
        producer: &PreparedSourceWatchProducer,
        contact: Option<&crate::source_contact_intake::PreparedSourceContact>,
        documents: Option<&crate::source_document_guards::PreparedDocGuards>,
    ) -> StorageResult<WatchSourceAcceptance> {
        self.accept_inner_outcome(
            request,
            None,
            None,
            contact,
            None,
            documents,
            Some(producer),
        )
        .await
    }

    /// Atomically accepts a freshly fetched revision under the original watch
    /// attempt, source/Doc authorization and optional original contact preparation.
    pub async fn accept_with_watch_producer(
        &self,
        request: AcceptSourceRevision,
        producer: &PreparedSourceWatchProducer,
        contact: Option<&crate::source_contact_intake::PreparedSourceContact>,
        documents: Option<&crate::source_document_guards::PreparedDocGuards>,
    ) -> StorageResult<OperationReceipt> {
        self.accept_inner(
            request,
            None,
            None,
            contact,
            None,
            documents,
            Some(producer),
        )
        .await
    }
}

#[cfg(test)]
#[path = "source_watch_producer_tests.rs"]
mod tests;
