//! Operator-driven recovery of work items that failed for a cause since fixed.
//!
//! `WorkItemStatus::Failed` is terminal, and correctly so: the retry path only
//! requeues errors that `is_transient_work_error` recognises, and a
//! misconfiguration is not transient. Retrying a config error forever would just
//! burn budget.
//!
//! But that leaves no way back. On 2026-07-30 a bad extraction model pin
//! (`gpt-5.6-sol`, which the vendored meerkat catalog cannot resolve) failed 470
//! `extract_assertions` items and 139 `triage_source` items on their first
//! attempt. Every one was a permanent hole in the knowledge base: 470 documents
//! ingested, rendered, planned, and then silently never extracted. Fixing the pin
//! did nothing for them, because nothing re-examines a terminal failure.
//!
//! So this pass exists to say "that cause is gone, try those again". It is driven
//! by a CUTOFF rather than a flag, which is what keeps it safe:
//!
//! - Only items whose last update predates the cutoff are requeued, so the
//!   operator names the moment the fix landed and nothing newer is touched.
//! - Requeuing moves `updated_at` to now, past the cutoff, so an item cannot be
//!   picked up twice by the same cutoff. If it fails again it stays failed.
//! - `attempts` is preserved rather than reset. The history is real, and the
//!   normal `max_attempts` ceiling still applies, so a genuinely broken item
//!   cannot be cycled indefinitely.

use chrono::{DateTime, Utc};
use elephant_storage::work_item_repository::WorkItemRepository;
use elephant_storage::StorageClient;
use elephant_types::records::work::WorkItemStatus;
use tracing::warn;

use crate::error::PipelineError;

/// Work kinds worth recovering, in dependency order.
///
/// Recovering an early stage first lets the chain re-derive naturally: a
/// requeued `triage_source` walks the whole way to extraction on its own.
const RECOVERABLE_KINDS: &[&str] = &[
    "triage_source",
    "render_doc",
    "plan_extraction",
    "extract_assertions",
    "commit_artifacts",
    "validate_artifacts",
];

/// How many items of each kind one pass will requeue.
///
/// Bounded so a large recovery lands over several sweeps instead of flooding the
/// queue in one go, which would spike provider spend without warning.
const DEFAULT_PER_KIND_LIMIT: usize = 200;

/// Attempt ceiling above which an item is left failed.
///
/// Mirrors `RetryPolicy::default().max_attempts`. WorkItem does not carry its own
/// policy, so the ceiling is applied here instead: without it, an item broken for
/// a reason the operator has NOT fixed could be requeued by every future cutoff.
const MAX_RECOVERY_ATTEMPTS: u32 = 3;

/// Outcome of one recovery pass.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FailedWorkRecoveryStats {
    /// Failed items examined.
    pub examined: usize,
    /// Items moved back to runnable.
    pub requeued: usize,
    /// Items left alone because they failed at or after the cutoff.
    pub too_recent: usize,
    /// Items left alone because they had already used every attempt.
    pub attempts_exhausted: usize,
}

impl FailedWorkRecoveryStats {
    fn merge(&mut self, other: &Self) {
        self.examined += other.examined;
        self.requeued += other.requeued;
        self.too_recent += other.too_recent;
        self.attempts_exhausted += other.attempts_exhausted;
    }
}

/// Reads the operator's recovery cutoff.
///
/// Absent or unparseable means the pass does not run. An unparseable value is
/// warned about rather than ignored: silently treating a typo as "off" is how an
/// operator ends up believing a recovery ran when it never did.
pub(crate) fn recovery_cutoff_from_env() -> Option<DateTime<Utc>> {
    let raw = std::env::var("PIPELINE_REQUEUE_FAILED_BEFORE").ok()?;
    parse_recovery_cutoff(&raw)
}

/// Reads the operator's infrastructure-cause match.
///
/// The attempt ceiling exists so an item broken for its OWN reason is not cycled
/// forever. It is wrong for items that used every attempt on an infrastructure
/// fault that has since been fixed: from 2026-09-12 Elephant's MCP status path
/// took 14-30 s, so extract_assertions exhausted all three attempts on
/// "Timed out connecting to 'elephant-mcp-agent'" within seconds, about 70 a day.
/// Those documents did nothing wrong. When the operator names that cause here, a
/// failed item whose last error contains it is requeued despite the ceiling and
/// its attempts are reset. The cutoff still applies, so each item is recovered
/// at most once per cutoff.
pub(crate) fn recovery_error_match_from_env() -> Option<String> {
    let raw = std::env::var("PIPELINE_REQUEUE_FAILED_ERROR_MATCH").ok()?;
    let trimmed = raw.trim();
    (!trimmed.is_empty()).then(|| trimmed.to_string())
}

/// Parses a cutoff, warning on anything it cannot read.
pub(crate) fn parse_recovery_cutoff(raw: &str) -> Option<DateTime<Utc>> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }
    match DateTime::parse_from_rfc3339(trimmed) {
        Ok(parsed) => Some(parsed.with_timezone(&Utc)),
        Err(err) => {
            warn!(
                "PIPELINE_REQUEUE_FAILED_BEFORE is not RFC3339 ({trimmed:?}: {err}); \
                 no failed work will be recovered"
            );
            None
        }
    }
}

/// Requeues terminally-failed work items that failed before `cutoff`.
///
/// # Errors
///
/// Returns an error if the work queue cannot be read.
pub async fn run_failed_work_recovery(
    storage: &StorageClient,
    space_id: &str,
    cutoff: DateTime<Utc>,
) -> Result<FailedWorkRecoveryStats, PipelineError> {
    run_failed_work_recovery_with_limit(
        storage,
        space_id,
        cutoff,
        DEFAULT_PER_KIND_LIMIT,
        recovery_error_match_from_env().as_deref(),
    )
    .await
}

/// As [`run_failed_work_recovery`], with an explicit per-kind limit for tests.
///
/// # Errors
///
/// Returns an error if the work queue cannot be read.
pub async fn run_failed_work_recovery_with_limit(
    storage: &StorageClient,
    space_id: &str,
    cutoff: DateTime<Utc>,
    per_kind_limit: usize,
    error_match: Option<&str>,
) -> Result<FailedWorkRecoveryStats, PipelineError> {
    let repo = WorkItemRepository::new(storage.clone());
    let mut stats = FailedWorkRecoveryStats::default();

    for kind in RECOVERABLE_KINDS {
        let kind_stats =
            recover_kind(&repo, space_id, kind, cutoff, per_kind_limit, error_match).await?;
        if kind_stats.requeued > 0 {
            // warn, not info: recovery is operator-armed and production runs
            // RUST_LOG=warn; an invisible recovery reads as a broken one.
            warn!(
                "Failed-work recovery requeued {} {kind} items that failed before {cutoff}",
                kind_stats.requeued
            );
        }
        stats.merge(&kind_stats);
    }

    if stats.requeued > 0 || stats.attempts_exhausted > 0 {
        warn!(
            "Failed-work recovery: {} examined, {} requeued, {} too recent, {} out of attempts",
            stats.examined, stats.requeued, stats.too_recent, stats.attempts_exhausted
        );
    }
    Ok(stats)
}

async fn recover_kind(
    repo: &WorkItemRepository,
    space_id: &str,
    kind: &str,
    cutoff: DateTime<Utc>,
    limit: usize,
    error_match: Option<&str>,
) -> Result<FailedWorkRecoveryStats, PipelineError> {
    let failed = repo
        .find_by_status_and_kind(space_id, WorkItemStatus::Failed, kind, limit)
        .await
        .map_err(PipelineError::StorageError)?;

    let mut stats = FailedWorkRecoveryStats::default();
    for item in failed {
        stats.examined += 1;

        if item.base.updated_at >= cutoff {
            stats.too_recent += 1;
            continue;
        }
        let infrastructure_cause = error_match.is_some_and(|needle| {
            item.last_error
                .as_deref()
                .is_some_and(|error| error.contains(needle))
        });
        if item.attempts >= MAX_RECOVERY_ATTEMPTS && !infrastructure_cause {
            stats.attempts_exhausted += 1;
            continue;
        }

        let id = item.base.id.clone();
        let mut requeued = item;
        if infrastructure_cause {
            // The attempts were spent on the named fault, not on this item.
            requeued.attempts = 0;
        }
        requeued.status = WorkItemStatus::Runnable;
        requeued.leased_by = None;
        requeued.lease_expires_at = None;
        // Clear the deferral so the item competes normally; the queue's own
        // budget handling will defer it again if there is no headroom.
        requeued.run_after = None;
        requeued.base.updated_at = Utc::now();

        // Conditional on still being Failed, so a concurrent worker or a second
        // recovery pass cannot double-requeue the same item.
        match repo
            .update_if_status(space_id, &id, requeued, WorkItemStatus::Failed)
            .await
        {
            Ok(Some(_)) => stats.requeued += 1,
            Ok(None) => {}
            Err(err) => {
                warn!("Failed-work recovery could not requeue {id}: {err}");
            }
        }
    }

    Ok(stats)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;

    #[test]
    fn absent_or_empty_cutoff_means_the_pass_does_not_run() {
        assert!(parse_recovery_cutoff("").is_none());
        assert!(parse_recovery_cutoff("   ").is_none());
    }

    #[test]
    fn unparseable_cutoff_does_not_silently_become_a_recovery() {
        // A typo must not be read as "recover everything" -- nor as a silent
        // no-op the operator mistakes for a completed run. It returns None and
        // warns.
        assert!(parse_recovery_cutoff("2026-07-30").is_none());
        assert!(parse_recovery_cutoff("yesterday").is_none());
        assert!(parse_recovery_cutoff("1753900000").is_none());
    }

    #[test]
    fn rfc3339_cutoff_is_read_as_utc() {
        let parsed = parse_recovery_cutoff("2026-07-30T21:00:00Z").expect("parsed");
        assert_eq!(parsed.to_rfc3339(), "2026-07-30T21:00:00+00:00");

        // An offset timestamp must normalise, or a cutoff written in local time
        // would silently select the wrong window.
        let offset = parse_recovery_cutoff("2026-07-30T23:00:00+02:00").expect("parsed");
        assert_eq!(offset, parsed);
    }

    #[test]
    fn recoverable_kinds_cover_the_whole_derivation_chain() {
        // extract_assertions is the one that actually failed 470 times; the
        // earlier stages matter because a doc that failed triage never reached
        // extraction at all.
        for required in ["triage_source", "render_doc", "extract_assertions"] {
            assert!(
                RECOVERABLE_KINDS.contains(&required),
                "{required} must be recoverable"
            );
        }
        // Maintenance and truth-maintenance kinds are deliberately absent: they
        // regenerate themselves on the next sweep, so requeuing them would
        // duplicate work rather than recover it.
        for excluded in ["tm_resolve_slot", "tm_commit", "ingest_metadata"] {
            assert!(
                !RECOVERABLE_KINDS.contains(&excluded),
                "{excluded} regenerates itself and must not be requeued"
            );
        }
    }

    use elephant_storage::Repository;
    use elephant_types::ids::TableName;
    use elephant_types::records::base::BaseFields;
    use elephant_types::records::work::WorkItem;
    use elephant_types::RecordId;

    const SPACE: &str = "test_space";

    const SCHEMA: &str = "DEFINE TABLE work_item SCHEMALESS;";

    fn failed_item(kind: &str, failed_at: DateTime<Utc>, attempts: u32) -> WorkItem {
        let mut base = BaseFields::new(
            RecordId::new_v4(TableName::WorkItem),
            SPACE.to_string(),
            "test".to_string(),
        );
        base.updated_at = failed_at;
        WorkItem {
            base,
            kind: kind.to_string(),
            payload: serde_json::json!({}),
            status: WorkItemStatus::Failed,
            priority: 100,
            idempotency_key: format!("idem-{}", uuid::Uuid::new_v4()),
            attempts,
            lease_expires_at: None,
            leased_by: None,
            run_after: None,
            last_error: Some("Cannot infer provider from model 'gpt-5.6-sol'".to_string()),
        }
    }

    async fn store_with(items: Vec<WorkItem>) -> (StorageClient, Vec<RecordId>) {
        let storage = StorageClient::new_memory().await.expect("storage");
        storage.query(SCHEMA).await.expect("schema");
        let repo = WorkItemRepository::new(storage.clone());
        let mut ids = Vec::new();
        for item in items {
            ids.push(item.base.id.clone());
            repo.create(item).await.expect("create work item");
        }
        (storage, ids)
    }

    async fn status_of(storage: &StorageClient, id: &RecordId) -> WorkItemStatus {
        WorkItemRepository::new(storage.clone())
            .get(SPACE, id)
            .await
            .expect("get")
            .expect("item exists")
            .status
    }

    fn mcp_timeout_item(failed_at: DateTime<Utc>) -> WorkItem {
        let mut item = failed_item("extract_assertions", failed_at, 3);
        item.last_error = Some(
            "extract_assertions failed: Extraction error: MCP connection failed: Protocol error: \
             Connection failed: Timed out connecting to 'elephant-mcp-agent' (10s)"
                .to_string(),
        );
        item
    }

    /// 2026-09-23: items that burnt every attempt on the MCP status stall come
    /// back when the operator names that cause, with their attempts reset.
    #[tokio::test]
    async fn a_named_infrastructure_cause_lifts_the_attempt_ceiling() {
        let failed_at = "2026-09-23T12:13:00Z".parse::<DateTime<Utc>>().unwrap();
        let cutoff = "2026-09-24T00:00:00Z".parse::<DateTime<Utc>>().unwrap();
        let (storage, ids) = store_with(vec![mcp_timeout_item(failed_at)]).await;

        let stats = run_failed_work_recovery_with_limit(
            &storage,
            SPACE,
            cutoff,
            DEFAULT_PER_KIND_LIMIT,
            Some("Timed out connecting to 'elephant-mcp-agent'"),
        )
        .await
        .expect("recovery");

        assert_eq!(stats.requeued, 1);
        assert_eq!(stats.attempts_exhausted, 0);
        let item = WorkItemRepository::new(storage.clone())
            .get(SPACE, &ids[0])
            .await
            .expect("get")
            .expect("item exists");
        assert_eq!(item.status, WorkItemStatus::Runnable);
        assert_eq!(item.attempts, 0);
    }

    /// Without the match, or with a different cause, the ceiling still holds:
    /// an item broken for its own reason is not cycled.
    #[tokio::test]
    async fn the_ceiling_holds_for_unnamed_causes() {
        let failed_at = "2026-09-23T12:13:00Z".parse::<DateTime<Utc>>().unwrap();
        let cutoff = "2026-09-24T00:00:00Z".parse::<DateTime<Utc>>().unwrap();
        let (storage, ids) = store_with(vec![
            mcp_timeout_item(failed_at),
            failed_item("extract_assertions", failed_at, 3),
        ])
        .await;

        let unarmed = run_failed_work_recovery_with_limit(
            &storage,
            SPACE,
            cutoff,
            DEFAULT_PER_KIND_LIMIT,
            None,
        )
        .await
        .expect("recovery");
        assert_eq!(unarmed.requeued, 0);
        assert_eq!(unarmed.attempts_exhausted, 2);

        let armed = run_failed_work_recovery_with_limit(
            &storage,
            SPACE,
            cutoff,
            DEFAULT_PER_KIND_LIMIT,
            Some("Timed out connecting to 'elephant-mcp-agent'"),
        )
        .await
        .expect("recovery");
        assert_eq!(armed.requeued, 1, "only the MCP-timeout item matches");
        assert_eq!(status_of(&storage, &ids[1]).await, WorkItemStatus::Failed);
    }

    /// The 470 extract_assertions failures from the bad model pin: failed before
    /// the fix landed, one attempt used, nothing else wrong with them.
    #[tokio::test]
    async fn failures_before_the_cutoff_are_requeued() {
        let failed_at = "2026-07-30T19:30:00Z".parse::<DateTime<Utc>>().unwrap();
        let cutoff = "2026-07-30T21:00:00Z".parse::<DateTime<Utc>>().unwrap();
        let (storage, ids) =
            store_with(vec![failed_item("extract_assertions", failed_at, 1)]).await;

        let stats = run_failed_work_recovery(&storage, SPACE, cutoff)
            .await
            .expect("recovery");

        assert_eq!(stats.requeued, 1);
        assert_eq!(status_of(&storage, &ids[0]).await, WorkItemStatus::Runnable);
    }

    /// Anything that failed at or after the cutoff failed for a reason the
    /// operator has NOT declared fixed, so it must be left alone.
    #[tokio::test]
    async fn failures_at_or_after_the_cutoff_are_left_alone() {
        let cutoff = "2026-07-30T21:00:00Z".parse::<DateTime<Utc>>().unwrap();
        let after = "2026-07-30T22:00:00Z".parse::<DateTime<Utc>>().unwrap();
        let (storage, ids) = store_with(vec![
            failed_item("extract_assertions", after, 1),
            failed_item("extract_assertions", cutoff, 1),
        ])
        .await;

        let stats = run_failed_work_recovery(&storage, SPACE, cutoff)
            .await
            .expect("recovery");

        assert_eq!(stats.requeued, 0);
        assert_eq!(stats.too_recent, 2, "the cutoff is exclusive on both");
        for id in &ids {
            assert_eq!(status_of(&storage, id).await, WorkItemStatus::Failed);
        }
    }

    /// An item that has burned every attempt stays failed, so a broken item
    /// cannot be cycled by every future cutoff.
    #[tokio::test]
    async fn items_out_of_attempts_stay_failed() {
        let failed_at = "2026-07-30T19:30:00Z".parse::<DateTime<Utc>>().unwrap();
        let cutoff = "2026-07-30T21:00:00Z".parse::<DateTime<Utc>>().unwrap();
        let (storage, ids) = store_with(vec![failed_item(
            "extract_assertions",
            failed_at,
            MAX_RECOVERY_ATTEMPTS,
        )])
        .await;

        let stats = run_failed_work_recovery(&storage, SPACE, cutoff)
            .await
            .expect("recovery");

        assert_eq!(stats.requeued, 0);
        assert_eq!(stats.attempts_exhausted, 1);
        assert_eq!(status_of(&storage, &ids[0]).await, WorkItemStatus::Failed);
    }

    /// Running the same cutoff twice must not requeue twice. Requeuing moves
    /// updated_at past the cutoff, which is what makes the pass self-limiting.
    #[tokio::test]
    async fn a_second_pass_with_the_same_cutoff_is_a_no_op() {
        let failed_at = "2026-07-30T19:30:00Z".parse::<DateTime<Utc>>().unwrap();
        let cutoff = "2026-07-30T21:00:00Z".parse::<DateTime<Utc>>().unwrap();
        let (storage, _) = store_with(vec![failed_item("extract_assertions", failed_at, 1)]).await;

        let first = run_failed_work_recovery(&storage, SPACE, cutoff)
            .await
            .expect("first");
        assert_eq!(first.requeued, 1);

        let second = run_failed_work_recovery(&storage, SPACE, cutoff)
            .await
            .expect("second");
        assert_eq!(second.requeued, 0, "already runnable, no longer failed");
    }

    /// Kinds that regenerate themselves must not be requeued, or recovery would
    /// duplicate work instead of restoring it.
    #[tokio::test]
    async fn non_recoverable_kinds_are_ignored() {
        let failed_at = "2026-07-30T19:30:00Z".parse::<DateTime<Utc>>().unwrap();
        let cutoff = "2026-07-30T21:00:00Z".parse::<DateTime<Utc>>().unwrap();
        let (storage, ids) = store_with(vec![failed_item("tm_resolve_slot", failed_at, 1)]).await;

        let stats = run_failed_work_recovery(&storage, SPACE, cutoff)
            .await
            .expect("recovery");

        assert_eq!(stats.examined, 0);
        assert_eq!(stats.requeued, 0);
        assert_eq!(status_of(&storage, &ids[0]).await, WorkItemStatus::Failed);
    }
}
