//! Ingestion-related work kinds for progressive ingestion.
//!
//! Per spec §19, the canonical pipeline includes:
//! - `ingest_source_stub`: creates source record
//! - `triage_source`: plugin decides mode, core enforces budgets
//! - `ingest_metadata`: stores metadata
//! - `upgrade_ingest_mode`: upgrades a source to a higher mode

use chrono::Utc;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use elephant_storage::ingest_decision_repository::IngestDecisionRepository;
use elephant_storage::{Repository, StorageClient, WorkItemRepository};
use elephant_types::ids::TableName;
use elephant_types::records::base::BaseFields;
use elephant_types::records::ingest::{DowngradePolicy, IngestDecision, IngestMode};
use elephant_types::records::source::Source;
use elephant_types::records::work::{WorkItem, WorkItemStatus};
use elephant_types::{Link, RecordId};

use crate::error::PipelineError;
use crate::scheduler::budget::{BudgetCheckResult, BudgetEnforcer};
use crate::scheduler::triage::{TriageContext, TriagePlugin};

/// Work kind identifier for `ingest_source_stub`.
pub const INGEST_SOURCE_STUB_KIND: &str = "ingest_source_stub";
/// Work kind identifier for `triage_source`.
pub const TRIAGE_SOURCE_KIND: &str = "triage_source";
/// Work kind identifier for `ingest_metadata`.
pub const INGEST_METADATA_KIND: &str = "ingest_metadata";
/// Work kind identifier for `upgrade_ingest_mode`.
pub const UPGRADE_INGEST_MODE_KIND: &str = "upgrade_ingest_mode";
/// Artifact type emitted by `ingest_metadata`.
pub const SOURCE_METADATA_ARTIFACT_TYPE: &str = "source_metadata";
/// Reserved source metadata key storing normalized source-intelligence output.
pub const SOURCE_NORMALIZED_METADATA_KEY: &str = "_normalized_source_metadata";

/// Enqueues the canonical `triage_source` work item for a source-first ingest flow.
///
/// # Errors
///
/// Returns an error if the work item cannot be created.
pub async fn enqueue_triage_source_work_item(
    storage: &StorageClient,
    source: &Source,
    actor: &str,
    priority: i32,
) -> Result<RecordId, PipelineError> {
    let repo = WorkItemRepository::new(storage.clone());
    let idempotency_key = format!("{TRIAGE_SOURCE_KIND}:{}", source.base.id);
    if let Some(mut existing) = repo
        .find_by_idempotency_key(&source.base.space_id, &idempotency_key)
        .await?
    {
        if matches!(
            existing.status,
            WorkItemStatus::Done | WorkItemStatus::Failed | WorkItemStatus::Cancelled
        ) {
            let existing_id = existing.base.id.clone();
            existing.base.security = source.base.security.clone();
            existing.base.provenance = source.base.provenance.clone();
            existing.base.subjects.clone_from(&source.base.subjects);
            existing.base.subjects_state = source.base.subjects_state;
            existing.status = WorkItemStatus::Runnable;
            existing.priority = priority;
            existing.attempts = 0;
            existing.lease_expires_at = None;
            existing.leased_by = None;
            existing.run_after = None;
            existing.last_error = None;
            existing.base.updated_at = Utc::now();
            let reopened = repo
                .update(&source.base.space_id, &existing_id, existing)
                .await?;
            return Ok(reopened.base.id);
        }
        return Ok(existing.base.id);
    }

    let deterministic_id = RecordId::new(
        TableName::WorkItem,
        Uuid::new_v5(
            &Uuid::NAMESPACE_OID,
            format!("{}|{idempotency_key}", source.base.space_id).as_bytes(),
        ),
    );
    let mut base = BaseFields::new(
        deterministic_id.clone(),
        source.base.space_id.clone(),
        actor.to_string(),
    );
    base.security = source.base.security.clone();
    base.provenance = source.base.provenance.clone();
    base.subjects.clone_from(&source.base.subjects);
    base.subjects_state = source.base.subjects_state;

    let work_item = WorkItem {
        base,
        kind: TRIAGE_SOURCE_KIND.to_string(),
        payload: serde_json::json!({
            "source_id": source.base.id.to_string(),
            "policy_version": "default",
        }),
        status: WorkItemStatus::Runnable,
        priority,
        idempotency_key: idempotency_key.clone(),
        attempts: 0,
        lease_expires_at: None,
        leased_by: None,
        run_after: None,
        last_error: None,
    };
    let id = work_item.base.id.clone();
    match repo.create(work_item).await {
        Ok(_) => Ok(id),
        Err(elephant_storage::StorageError::Conflict(_)) => {
            let existing = repo
                .find_by_idempotency_key(&source.base.space_id, &idempotency_key)
                .await?;
            existing.map(|item| item.base.id).ok_or_else(|| {
                PipelineError::InvalidState(
                    "triage_source idempotency conflict without existing work item".to_string(),
                )
            })
        }
        Err(err) => Err(err.into()),
    }
}

/// Result of the `ingest_source_stub` work kind.
#[derive(Debug, Clone)]
pub struct IngestSourceStubResult {
    /// The created source ID.
    pub source_id: RecordId,
    /// Whether the source was newly created or already existed.
    pub created: bool,
}

/// Result of the `triage_source` work kind.
#[derive(Debug, Clone)]
pub struct TriageSourceResult {
    /// The ingest decision ID.
    pub decision_id: RecordId,
    /// The decided ingestion mode.
    pub decided_mode: IngestMode,
    /// Whether the mode was downgraded due to budget.
    pub was_downgraded: bool,
    /// Original mode before downgrade (if downgraded).
    pub original_mode: Option<IngestMode>,
    /// Reason for downgrade (if downgraded).
    pub downgrade_reason: Option<String>,
}

/// Result of the `ingest_metadata` work kind.
#[derive(Debug, Clone)]
pub struct IngestMetadataResult {
    /// The source ID that was updated.
    pub source_id: RecordId,
    /// Normalized metadata artifact derived for the source.
    pub metadata_artifact: SourceMetadataArtifactData,
}

/// Stored payload for machine-derived source metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceMetadataArtifactData {
    /// The source this metadata belongs to.
    pub source_id: String,
    /// Normalized source title or label.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub normalized_title: Option<String>,
    /// Normalized author string.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub normalized_author: Option<String>,
    /// Normalized channel/platform string.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub normalized_channel: Option<String>,
    /// Normalized source timestamp.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub normalized_timestamp: Option<chrono::DateTime<chrono::Utc>>,
    /// Normalized external identifiers for the source.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub normalized_external_ids: Vec<String>,
    /// Normalized canonical URI/path for the source.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub normalized_uri: Option<String>,
    /// Sanitized metadata patch derived from the source payload.
    pub raw_metadata_patch: serde_json::Value,
}

impl SourceMetadataArtifactData {
    /// Derives normalized source metadata from a source record.
    #[must_use]
    pub fn derive(source: &Source) -> Self {
        let normalized_title = source
            .manual_doc_template()
            .ok()
            .flatten()
            .map(|template| template.title)
            .or_else(|| metadata_string(&source.metadata, &["title", "name", "subject"]));
        let normalized_title = normalize_optional_string(normalized_title);

        let normalized_author = normalize_optional_string(
            source
                .author
                .clone()
                .or_else(|| metadata_string(&source.metadata, &["author", "sender", "from"])),
        );

        let normalized_channel = normalize_optional_string(
            source
                .channel
                .clone()
                .or_else(|| metadata_string(&source.metadata, &["channel", "platform"])),
        );

        let normalized_timestamp = source.timestamp.or_else(|| {
            metadata_string(
                &source.metadata,
                &["timestamp", "time", "published_at", "created_at"],
            )
            .and_then(|value| chrono::DateTime::parse_from_rfc3339(&value).ok())
            .map(|value| value.with_timezone(&chrono::Utc))
        });

        let normalized_uri = normalize_optional_string(
            source
                .uri
                .clone()
                .or_else(|| source.path.clone())
                .or_else(|| metadata_string(&source.metadata, &["uri", "url", "path"])),
        );

        Self {
            source_id: source.base.id.to_string(),
            normalized_title,
            normalized_author,
            normalized_channel,
            normalized_timestamp,
            normalized_external_ids: collect_external_ids(source),
            normalized_uri,
            raw_metadata_patch: sanitized_source_metadata_patch(&source.metadata),
        }
    }
}

/// Result of the `upgrade_ingest_mode` work kind.
#[derive(Debug, Clone)]
pub struct UpgradeIngestModeResult {
    /// The source ID that was upgraded.
    pub source_id: RecordId,
    /// The previous mode.
    pub previous_mode: IngestMode,
    /// The new mode.
    pub new_mode: IngestMode,
    /// Whether upgrade was allowed by budget.
    pub allowed: bool,
    /// Reason if upgrade was denied.
    pub denied_reason: Option<String>,
}

/// Executes the `ingest_source_stub` work kind.
///
/// Creates a source record stub for later processing.
///
/// # Errors
///
/// Returns an error if the source cannot be created.
pub async fn execute_ingest_source_stub(
    source: Source,
    _storage: &StorageClient,
) -> Result<IngestSourceStubResult, PipelineError> {
    // In a real implementation, this would:
    // 1. Check for existing source by external_id
    // 2. Create if not exists
    // 3. Return the source ID

    Ok(IngestSourceStubResult {
        source_id: source.base.id,
        created: true,
    })
}

/// Executes the `triage_source` work kind.
///
/// Per spec §6.3: Calls triage plugin and enforces budgets.
///
/// # Errors
///
/// Returns an error if triage fails.
pub async fn execute_triage_source<T: TriagePlugin>(
    source: &Source,
    triage_plugin: &T,
    budget_enforcer: &BudgetEnforcer,
    decision_repo: &IngestDecisionRepository,
    space_id: &str,
    policy_version: &str,
    actor: &str,
) -> Result<TriageSourceResult, PipelineError> {
    execute_triage_source_inner(
        source,
        triage_plugin,
        budget_enforcer,
        decision_repo,
        space_id,
        policy_version,
        actor,
        true,
    )
    .await
}

/// Plan triage inside an already-accounted worker attempt. Reservations and
/// physical usage belong to its WorkRun; the decision records estimates only.
/// The caller retains responsibility for a fenced continuation on budget denial.
///
/// # Errors
/// Returns triage, budget-read, per-item-cap, or decision persistence failures.
pub async fn execute_triage_source_for_work_attempt<T: TriagePlugin>(
    source: &Source,
    triage_plugin: &T,
    budget_enforcer: &BudgetEnforcer,
    decision_repo: &IngestDecisionRepository,
    space_id: &str,
    policy_version: &str,
    actor: &str,
) -> Result<TriageSourceResult, PipelineError> {
    execute_triage_source_inner(
        source,
        triage_plugin,
        budget_enforcer,
        decision_repo,
        space_id,
        policy_version,
        actor,
        false,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn execute_triage_source_inner<T: TriagePlugin>(
    source: &Source,
    triage_plugin: &T,
    budget_enforcer: &BudgetEnforcer,
    decision_repo: &IngestDecisionRepository,
    space_id: &str,
    policy_version: &str,
    actor: &str,
    reserve_triage: bool,
) -> Result<TriageSourceResult, PipelineError> {
    // 1. Create budget snapshot
    let budget_snapshot = budget_enforcer.create_snapshot().await?;

    // 2. Create triage context
    let ctx = TriageContext {
        source: source.clone(),
        budget_snapshot: budget_snapshot.clone(),
        space_id: space_id.to_string(),
        policy_version: policy_version.to_string(),
    };

    // 3. Call triage plugin
    let triage_result = triage_plugin.triage(ctx).await?;

    // 4. Check budget for the decided mode (daily budgets + per-item caps + concurrency)
    let manifest = triage_plugin.manifest();
    let worker_kind = manifest
        .work_kinds
        .first()
        .map(String::as_str)
        .unwrap_or("triage_source");
    let estimated_tokens = manifest.max_tokens.unwrap_or(0);
    let estimated_wall_time_secs = manifest.max_wall_time_secs.unwrap_or(0);
    let current_running = budget_enforcer
        .current_running_for_kind(worker_kind)
        .await?;

    let budget_check = if reserve_triage {
        budget_enforcer
            .check_all_budgets_and_reserve(
                triage_result.decision,
                triage_result.estimated_cost,
                estimated_tokens,
                estimated_wall_time_secs,
                worker_kind,
                current_running,
            )
            .await?
    } else {
        budget_enforcer
            .check_all_budgets(
                triage_result.decision,
                triage_result.estimated_cost,
                estimated_tokens,
                estimated_wall_time_secs,
                worker_kind,
                current_running,
            )
            .await?
    };

    if let Some(cap_breach) = budget_check.cap_breach {
        return Err(match cap_breach {
            crate::scheduler::budget::CapBreachResult::TokenCapExceeded { actual, limit } => {
                PipelineError::PerItemCapExceeded {
                    cap_type: "tokens".to_string(),
                    actual,
                    limit,
                }
            }
            crate::scheduler::budget::CapBreachResult::WallTimeExceeded { actual, limit } => {
                PipelineError::PerItemCapExceeded {
                    cap_type: "wall_time".to_string(),
                    actual,
                    limit,
                }
            }
            crate::scheduler::budget::CapBreachResult::WithinCaps => {
                PipelineError::BudgetExceeded("Per-item caps reported as within limits".to_string())
            }
        });
    }

    // 5. Apply budget decision
    let (final_mode, was_downgraded, original_mode, downgrade_reason) =
        match budget_check.result.clone() {
            BudgetCheckResult::Allowed => (triage_result.decision, false, None, None),
            BudgetCheckResult::Downgrade {
                target_mode,
                reason,
            } => (
                target_mode,
                true,
                Some(triage_result.decision),
                Some(reason),
            ),
            BudgetCheckResult::Rejected { reason } => {
                // Reject means skip mode
                (
                    IngestMode::Skip,
                    true,
                    Some(triage_result.decision),
                    Some(reason),
                )
            }
            BudgetCheckResult::Queued { worker_kind } => {
                // Queued items still get triaged but marked for later
                (
                    triage_result.decision,
                    false,
                    None,
                    Some(format!("queued:{worker_kind}")),
                )
            }
        };

    if reserve_triage
        && was_downgraded
        && budget_enforcer.downgrade_policy() == DowngradePolicy::DegradeModeWithQueue
    {
        if let (Some(original), Some(reason)) = (original_mode, downgrade_reason.as_deref()) {
            budget_enforcer
                .queue_mode_upgrade(
                    &source.base.id,
                    final_mode,
                    original,
                    triage_result.estimated_cost,
                    actor,
                    reason,
                )
                .await?;
        }
    }

    // 6. Reserve estimated cost for the decided mode
    let cost_ledger_id = if !reserve_triage || final_mode == IngestMode::Skip {
        None
    } else if budget_check.reserved_ledger_id.is_some() {
        budget_check.reserved_ledger_id
    } else {
        Some(
            budget_enforcer
                .reserve_cost(final_mode, "triage", triage_result.estimated_cost)
                .await?,
        )
    };

    // 7. Create ingest decision record
    let decision_id = RecordId::new_v4(TableName::IngestDecision);
    let decision = IngestDecision {
        base: BaseFields::new(decision_id.clone(), space_id.to_string(), actor.to_string()),
        source: Link::new(source.base.id.clone()),
        decision: final_mode,
        reason_codes: triage_result.reason_codes,
        confidence: triage_result.confidence,
        estimated_cost: triage_result.estimated_cost,
        cost_ledger_id,
        budget_snapshot: Some(budget_snapshot),
        policy_version: Some(policy_version.to_string()),
        downgrade_reason: downgrade_reason.clone(),
        original_mode,
    };

    decision_repo
        .create(decision)
        .await
        .map_err(PipelineError::StorageError)?;

    Ok(TriageSourceResult {
        decision_id,
        decided_mode: final_mode,
        was_downgraded,
        original_mode,
        downgrade_reason,
    })
}

/// Executes the `ingest_metadata` work kind.
///
/// Extracts and stores metadata for a source.
///
/// # Errors
///
/// Returns an error if metadata extraction fails.
pub async fn execute_ingest_metadata(
    source: &Source,
    _storage: &StorageClient,
) -> Result<IngestMetadataResult, PipelineError> {
    let metadata_artifact = SourceMetadataArtifactData::derive(source);

    Ok(IngestMetadataResult {
        source_id: source.base.id.clone(),
        metadata_artifact,
    })
}

fn metadata_string(metadata: &serde_json::Value, keys: &[&str]) -> Option<String> {
    keys.iter()
        .find_map(|key| metadata.get(*key))
        .and_then(serde_json::Value::as_str)
        .map(ToOwned::to_owned)
}

fn normalize_optional_string(value: Option<String>) -> Option<String> {
    value.and_then(|candidate| {
        let trimmed = candidate.trim();
        (!trimmed.is_empty()).then(|| trimmed.to_string())
    })
}

fn collect_external_ids(source: &Source) -> Vec<String> {
    let mut ids = Vec::new();
    if let Some(external_id) = normalize_optional_string(source.external_id.clone()) {
        ids.push(external_id);
    }

    if let Some(values) = source
        .metadata
        .get("external_ids")
        .and_then(serde_json::Value::as_array)
    {
        ids.extend(
            values
                .iter()
                .filter_map(serde_json::Value::as_str)
                .filter_map(|value| normalize_optional_string(Some(value.to_string()))),
        );
    } else if let Some(external_id) = metadata_string(&source.metadata, &["external_id"]) {
        if let Some(normalized) = normalize_optional_string(Some(external_id)) {
            ids.push(normalized);
        }
    }

    ids.sort();
    ids.dedup();
    ids
}

fn sanitized_source_metadata_patch(metadata: &serde_json::Value) -> serde_json::Value {
    let mut patch = metadata.clone();
    if let Some(object) = patch.as_object_mut() {
        object.remove(elephant_types::records::source::SOURCE_REQUESTED_INGEST_MODE_KEY);
        object.remove(elephant_types::records::source::SOURCE_MANUAL_DOC_TEMPLATE_KEY);
        object.remove(SOURCE_NORMALIZED_METADATA_KEY);
    }
    patch
}

/// Executes the `upgrade_ingest_mode` work kind.
///
/// Per spec §6.5: Agents can request Elephant to deepen only relevant sources.
///
/// # Errors
///
/// Returns an error if the upgrade fails.
pub async fn execute_upgrade_ingest_mode(
    source: &Source,
    current_mode: IngestMode,
    target_mode: IngestMode,
    budget_enforcer: &BudgetEnforcer,
    estimated_cost: f64,
    reason: &str,
) -> Result<UpgradeIngestModeResult, PipelineError> {
    execute_upgrade_ingest_mode_inner(
        source,
        current_mode,
        target_mode,
        budget_enforcer,
        estimated_cost,
        reason,
        ModeAdmission::StrictUpgrade,
    )
    .await
}

/// Checks materialization of an accepted source revision, including rebuilding
/// at its existing Retrieval or Graph mode. Its caller retains the original
/// publication attempt and configuration through rendering and finalization.
/// This path never creates legacy upgrade work; the caller retries its own item.
///
/// # Errors
/// Returns invalid transitions, budget reads, and per-item-cap failures.
pub async fn execute_upgrade_ingest_mode_for_work_attempt(
    source: &Source,
    current_mode: IngestMode,
    target_mode: IngestMode,
    budget_enforcer: &BudgetEnforcer,
    estimated_cost: f64,
    reason: &str,
) -> Result<UpgradeIngestModeResult, PipelineError> {
    execute_upgrade_ingest_mode_inner(
        source,
        current_mode,
        target_mode,
        budget_enforcer,
        estimated_cost,
        reason,
        ModeAdmission::AcceptedRevision,
    )
    .await
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ModeAdmission {
    StrictUpgrade,
    AcceptedRevision,
}

async fn execute_upgrade_ingest_mode_inner(
    source: &Source,
    current_mode: IngestMode,
    target_mode: IngestMode,
    budget_enforcer: &BudgetEnforcer,
    estimated_cost: f64,
    _reason: &str,
    admission: ModeAdmission,
) -> Result<UpgradeIngestModeResult, PipelineError> {
    // An immutable accepted replacement can retain the published mode while
    // still requiring its own materialization. Legacy requests only deepen.
    let rebuild_accepted_revision = admission == ModeAdmission::AcceptedRevision
        && current_mode == target_mode
        && matches!(target_mode, IngestMode::Retrieval | IngestMode::Graph);
    if !is_valid_upgrade(current_mode, target_mode) && !rebuild_accepted_revision {
        return Err(PipelineError::InvalidStateTransition {
            from: current_mode.to_string(),
            to: target_mode.to_string(),
        });
    }

    // Check budget for target mode (daily budgets + per-item caps + concurrency)
    let worker_kind = "upgrade_ingest_mode";
    let current_running = budget_enforcer
        .current_running_for_kind(worker_kind)
        .await?;
    let budget_check = budget_enforcer
        .check_all_budgets(
            target_mode,
            estimated_cost,
            0,
            0,
            worker_kind,
            current_running,
        )
        .await?;

    if let Some(cap_breach) = budget_check.cap_breach {
        return Err(match cap_breach {
            crate::scheduler::budget::CapBreachResult::TokenCapExceeded { actual, limit } => {
                PipelineError::PerItemCapExceeded {
                    cap_type: "tokens".to_string(),
                    actual,
                    limit,
                }
            }
            crate::scheduler::budget::CapBreachResult::WallTimeExceeded { actual, limit } => {
                PipelineError::PerItemCapExceeded {
                    cap_type: "wall_time".to_string(),
                    actual,
                    limit,
                }
            }
            crate::scheduler::budget::CapBreachResult::WithinCaps => {
                PipelineError::BudgetExceeded("Per-item caps reported as within limits".to_string())
            }
        });
    }

    match budget_check.result {
        BudgetCheckResult::Allowed => Ok(UpgradeIngestModeResult {
            source_id: source.base.id.clone(),
            previous_mode: current_mode,
            new_mode: target_mode,
            allowed: true,
            denied_reason: None,
        }),
        BudgetCheckResult::Rejected { reason } | BudgetCheckResult::Downgrade { reason, .. } => {
            if admission == ModeAdmission::StrictUpgrade
                && budget_enforcer.downgrade_policy() == DowngradePolicy::DegradeModeWithQueue
            {
                let _ = budget_enforcer
                    .queue_mode_upgrade(
                        &source.base.id,
                        current_mode,
                        target_mode,
                        estimated_cost,
                        "budget_enforcer",
                        &reason,
                    )
                    .await?;
            }
            Ok(UpgradeIngestModeResult {
                source_id: source.base.id.clone(),
                previous_mode: current_mode,
                new_mode: current_mode, // Stay at current mode
                allowed: false,
                denied_reason: Some(reason),
            })
        }
        BudgetCheckResult::Queued { worker_kind } => Ok(UpgradeIngestModeResult {
            source_id: source.base.id.clone(),
            previous_mode: current_mode,
            new_mode: current_mode,
            allowed: false,
            denied_reason: Some(format!("Queued due to concurrency limit: {worker_kind}")),
        }),
    }
}

/// Checks if the mode upgrade is valid (only to higher modes).
fn is_valid_upgrade(from: IngestMode, to: IngestMode) -> bool {
    let mode_order = |m: IngestMode| match m {
        IngestMode::Skip => 0,
        IngestMode::Metadata => 1,
        IngestMode::Retrieval => 2,
        IngestMode::Graph => 3,
    };
    mode_order(to) > mode_order(from)
}

/// Partial artifact metadata.
///
/// Per spec §17.6: If a single item exceeds caps, worker must abort and return partial results.
/// The partial flag is stored in the data payload's metadata.
#[derive(Debug, Clone)]
pub struct PartialArtifactData {
    /// The actual artifact data.
    pub data: serde_json::Value,
    /// Whether this is a partial result.
    pub partial: bool,
    /// Reason for partial result.
    pub partial_reason: Option<String>,
}

impl PartialArtifactData {
    /// Creates partial artifact data.
    pub fn new(data: serde_json::Value, reason: &str) -> Self {
        Self {
            data,
            partial: true,
            partial_reason: Some(reason.to_string()),
        }
    }

    /// Creates complete artifact data.
    pub fn complete(data: serde_json::Value) -> Self {
        Self {
            data,
            partial: false,
            partial_reason: None,
        }
    }

    /// Converts to JSON value for storage.
    pub fn to_artifact_data(&self) -> serde_json::Value {
        serde_json::json!({
            "data": self.data,
            "partial": self.partial,
            "partial_reason": self.partial_reason
        })
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;
    use crate::scheduler::triage::{DefaultTriagePlugin, TriageResult};
    use elephant_storage::work_item_repository::WorkItemRepository;
    use elephant_types::records::ingest::Budget;
    use elephant_types::records::source::SourceType;
    use elephant_types::records::work::PluginManifest;
    use elephant_types::records::work::RetryPolicy;
    use elephant_types::records::work::WorkItem;
    use elephant_types::records::work::WorkItemStatus;

    const SCHEMA: &str = r"
        DEFINE TABLE ingest_decision SCHEMALESS;
        DEFINE TABLE cost_ledger_daily SCHEMALESS;
        DEFINE TABLE work_item SCHEMALESS;
    ";

    fn make_source() -> Source {
        Source::new(
            BaseFields::new(
                RecordId::new_v4(TableName::Source),
                "test_space".to_string(),
                "test".to_string(),
            ),
            SourceType::Web,
            "test-hash-123".to_string(),
        )
    }

    #[derive(Debug, Clone)]
    struct GraphTriagePlugin {
        manifest: PluginManifest,
        source_types: Vec<String>,
    }

    impl GraphTriagePlugin {
        fn new() -> Self {
            Self {
                manifest: PluginManifest {
                    plugin_id: "graph_triage".to_string(),
                    version: "1.0.0".to_string(),
                    plugin_type: "triage".to_string(),
                    required_scopes: vec![],
                    tool_access: vec![],
                    work_kinds: vec!["triage_source".to_string()],
                    determinism: elephant_types::records::work::Determinism::Deterministic,
                    max_tokens: None,
                    max_wall_time_secs: None,
                    input_types: vec!["source".to_string()],
                    output_types: vec!["ingest_decision".to_string()],
                    validator_profile: None,
                    config_schema: None,
                    health_check: None,
                    retry_policy: RetryPolicy::default(),
                },
                source_types: vec!["*".to_string()],
            }
        }
    }

    #[derive(Debug, Clone)]
    struct CapBreachTriagePlugin {
        manifest: PluginManifest,
        source_types: Vec<String>,
    }

    impl CapBreachTriagePlugin {
        fn new() -> Self {
            Self {
                manifest: PluginManifest {
                    plugin_id: "cap_breach_triage".to_string(),
                    version: "1.0.0".to_string(),
                    plugin_type: "triage".to_string(),
                    required_scopes: vec![],
                    tool_access: vec![],
                    work_kinds: vec!["triage_source".to_string()],
                    determinism: elephant_types::records::work::Determinism::LlmSingleShot,
                    max_tokens: Some(1000),
                    max_wall_time_secs: Some(30),
                    input_types: vec!["source".to_string()],
                    output_types: vec!["ingest_decision".to_string()],
                    validator_profile: None,
                    config_schema: None,
                    health_check: None,
                    retry_policy: RetryPolicy::default(),
                },
                source_types: vec!["*".to_string()],
            }
        }
    }

    #[async_trait::async_trait]
    impl TriagePlugin for GraphTriagePlugin {
        fn manifest(&self) -> &PluginManifest {
            &self.manifest
        }

        fn source_types(&self) -> &[String] {
            &self.source_types
        }

        async fn triage(&self, _ctx: TriageContext) -> Result<TriageResult, PipelineError> {
            Ok(TriageResult {
                decision: IngestMode::Graph,
                reason_codes: vec!["test_graph".to_string()],
                confidence: 1.0,
                estimated_cost: 5.0,
            })
        }
    }

    #[async_trait::async_trait]
    impl TriagePlugin for CapBreachTriagePlugin {
        fn manifest(&self) -> &PluginManifest {
            &self.manifest
        }

        fn source_types(&self) -> &[String] {
            &self.source_types
        }

        async fn triage(&self, _ctx: TriageContext) -> Result<TriageResult, PipelineError> {
            Ok(TriageResult {
                decision: IngestMode::Retrieval,
                reason_codes: vec!["cap_breach".to_string()],
                confidence: 1.0,
                estimated_cost: 1.0,
            })
        }
    }

    #[test]
    fn test_is_valid_upgrade() {
        // Valid upgrades
        assert!(is_valid_upgrade(IngestMode::Skip, IngestMode::Metadata));
        assert!(is_valid_upgrade(IngestMode::Skip, IngestMode::Retrieval));
        assert!(is_valid_upgrade(IngestMode::Skip, IngestMode::Graph));
        assert!(is_valid_upgrade(
            IngestMode::Metadata,
            IngestMode::Retrieval
        ));
        assert!(is_valid_upgrade(IngestMode::Metadata, IngestMode::Graph));
        assert!(is_valid_upgrade(IngestMode::Retrieval, IngestMode::Graph));

        // Invalid upgrades (same or lower mode)
        assert!(!is_valid_upgrade(IngestMode::Graph, IngestMode::Graph));
        assert!(!is_valid_upgrade(IngestMode::Graph, IngestMode::Retrieval));
        assert!(!is_valid_upgrade(
            IngestMode::Retrieval,
            IngestMode::Metadata
        ));
        assert!(!is_valid_upgrade(IngestMode::Metadata, IngestMode::Skip));
    }

    #[tokio::test]
    async fn test_execute_ingest_source_stub() {
        let storage = StorageClient::new_memory().await.expect("storage");
        let source = make_source();

        let result = execute_ingest_source_stub(source.clone(), &storage)
            .await
            .expect("stub");

        assert_eq!(result.source_id, source.base.id);
        assert!(result.created);
    }

    #[tokio::test]
    async fn test_enqueue_triage_source_work_item_reopens_terminal_item() {
        let storage = StorageClient::new_memory().await.expect("storage");
        storage.query(SCHEMA).await.expect("schema");
        let mut source = make_source();

        let first_id = enqueue_triage_source_work_item(&storage, &source, "tester", 5)
            .await
            .expect("enqueue first");
        let repo = WorkItemRepository::new(storage.clone());
        repo.update_status(&source.base.space_id, &first_id, WorkItemStatus::Done)
            .await
            .expect("mark done");

        source.base.security.level = 7;
        source.base.provenance.sources =
            vec![RecordId::new_v4(elephant_types::ids::TableName::Source)];
        source.base.subjects = vec![Link::new(RecordId::new_v4(
            elephant_types::ids::TableName::Entity,
        ))];
        let second_id = enqueue_triage_source_work_item(&storage, &source, "tester", 7)
            .await
            .expect("enqueue second");
        assert_eq!(first_id, second_id);

        let item = repo
            .get(&source.base.space_id, &second_id)
            .await
            .expect("load work item")
            .expect("persisted work item");
        assert_eq!(item.status, WorkItemStatus::Runnable);
        assert_eq!(item.priority, 7);
        assert_eq!(item.attempts, 0);
        assert!(item.lease_expires_at.is_none());
        assert!(item.leased_by.is_none());
        assert!(item.last_error.is_none());
        assert_eq!(item.base.security.level, 7);
        assert_eq!(item.base.provenance.sources.len(), 1);
        assert_eq!(item.base.subjects.len(), 1);
    }

    #[tokio::test]
    async fn test_execute_triage_source() {
        let storage = StorageClient::new_memory().await.expect("storage");
        storage.query(SCHEMA).await.expect("schema");

        let source = make_source();
        let triage_plugin = DefaultTriagePlugin::new();
        let budget = Budget::default();
        let budget_enforcer =
            BudgetEnforcer::new(storage.clone(), budget, "test_space".to_string());
        let decision_repo = IngestDecisionRepository::new(storage);

        let result = execute_triage_source(
            &source,
            &triage_plugin,
            &budget_enforcer,
            &decision_repo,
            "test_space",
            "v1.0",
            "test_actor",
        )
        .await
        .expect("triage");

        assert_eq!(result.decided_mode, IngestMode::Metadata);
        assert!(!result.was_downgraded);
    }

    async fn attempt_triage_accounting_contract(storage: StorageClient) {
        if storage.embedded_store().is_none() {
            storage.query(SCHEMA).await.expect("schema");
        }
        let source = make_source();
        let budget = BudgetEnforcer::new(storage.clone(), Budget::default(), "test_space".into());
        let original_ledger = budget
            .reserve_cost(IngestMode::Graph, "work_attempt", 1.0)
            .await
            .expect("original attempt reservation");
        let ledger = elephant_storage::CostLedgerDailyRepository::new(storage.clone());
        let before = ledger.list("test_space", 100, None).await.expect("before");
        let decisions = IngestDecisionRepository::new(storage.clone());
        let result = execute_triage_source_for_work_attempt(
            &source,
            &GraphTriagePlugin::new(),
            &budget,
            &decisions,
            "test_space",
            "v1",
            "fixture",
        )
        .await
        .expect("attempt triage");
        let decision = decisions
            .get("test_space", &result.decision_id)
            .await
            .expect("get")
            .expect("decision");
        assert!((decision.estimated_cost - 5.0).abs() < f64::EPSILON);
        assert!(
            decision.cost_ledger_id.is_none(),
            "decision cannot create an unowned reservation"
        );
        let after = ledger.list("test_space", 100, None).await.expect("after");
        assert_eq!(before.len(), after.len());
        let original = ledger
            .get("test_space", &original_ledger)
            .await
            .expect("original")
            .expect("retained");
        assert!((original.reserved_euros - 1.0).abs() < f64::EPSILON);
        assert!(original.reconciled_euros.abs() < f64::EPSILON);
        let denied_budget = BudgetEnforcer::new(
            storage.clone(),
            Budget {
                daily_euros_graph: 1.0,
                downgrade_policy: DowngradePolicy::DegradeModeWithQueue,
                ..Budget::default()
            },
            "test_space".into(),
        );
        let denied = execute_triage_source_for_work_attempt(
            &source,
            &GraphTriagePlugin::new(),
            &denied_budget,
            &decisions,
            "test_space",
            "v1",
            "fixture",
        )
        .await
        .expect("record denied planning");
        assert!(denied.was_downgraded);
        let upgrade = execute_upgrade_ingest_mode_for_work_attempt(
            &source,
            IngestMode::Metadata,
            IngestMode::Graph,
            &denied_budget,
            5.0,
            "fixture",
        )
        .await
        .expect("denied upgrade");
        assert!(!upgrade.allowed);
        assert!(upgrade.denied_reason.is_some());
        assert!(
            WorkItemRepository::new(storage)
                .list("test_space", 100, None)
                .await
                .expect("queued work")
                .is_empty(),
            "accepted worker must retain its original publication on continuations"
        );
    }

    #[tokio::test]
    async fn embedded_attempt_triage_does_not_reserve_future_work_or_enqueue_unfenced_upgrade() {
        attempt_triage_accounting_contract(StorageClient::new_embedded().await.expect("embedded"))
            .await;
    }
    #[tokio::test]
    async fn surreal_attempt_triage_does_not_reserve_future_work_or_enqueue_unfenced_upgrade() {
        attempt_triage_accounting_contract(StorageClient::new_memory().await.expect("memory"))
            .await;
    }

    async fn accepted_revision_same_mode_contract(storage: StorageClient) {
        if storage.embedded_store().is_none() {
            storage.query(SCHEMA).await.expect("schema");
        }
        let allowed_budget =
            BudgetEnforcer::new(storage.clone(), Budget::default(), "test_space".into());
        let denied_budget = BudgetEnforcer::new(
            storage.clone(),
            Budget {
                daily_euros_retrieval: 0.5,
                daily_euros_graph: 0.5,
                downgrade_policy: DowngradePolicy::DegradeModeWithQueue,
                ..Budget::default()
            },
            "test_space".into(),
        );
        for mode in [IngestMode::Retrieval, IngestMode::Graph] {
            let mut source = make_source();
            source.current_ingest_mode = Some(mode);
            let original = serde_json::to_value(&source).expect("original source");
            let admitted = execute_upgrade_ingest_mode_for_work_attempt(
                &source,
                mode,
                mode,
                &allowed_budget,
                1.0,
                "accepted replacement",
            )
            .await
            .expect("same-mode accepted revision requires materialization");
            assert!(admitted.allowed);
            assert_eq!(admitted.source_id, source.base.id);
            assert_eq!(admitted.previous_mode, mode);
            assert_eq!(admitted.new_mode, mode);
            assert!(admitted.denied_reason.is_none());

            let legacy = execute_upgrade_ingest_mode(
                &source,
                mode,
                mode,
                &allowed_budget,
                1.0,
                "legacy upgrade",
            )
            .await;
            assert!(matches!(
                legacy,
                Err(PipelineError::InvalidStateTransition { from, to })
                    if from == mode.to_string() && to == mode.to_string()
            ));

            let denied = execute_upgrade_ingest_mode_for_work_attempt(
                &source,
                mode,
                mode,
                &denied_budget,
                1.0,
                "accepted replacement exceeds daily budget",
            )
            .await
            .expect("same-mode rebuild still checks budget");
            assert!(!denied.allowed);
            assert_eq!(denied.previous_mode, mode);
            assert_eq!(denied.new_mode, mode);
            assert!(denied.denied_reason.is_some());
            assert_eq!(
                serde_json::to_value(&source).expect("retained source"),
                original
            );
        }

        let source = make_source();
        for (current, target) in [
            (IngestMode::Graph, IngestMode::Retrieval),
            (IngestMode::Retrieval, IngestMode::Metadata),
            (IngestMode::Metadata, IngestMode::Metadata),
            (IngestMode::Skip, IngestMode::Skip),
        ] {
            assert!(matches!(
                execute_upgrade_ingest_mode_for_work_attempt(
                    &source, current, target, &allowed_budget, 1.0, "invalid accepted transition",
                )
                .await,
                Err(PipelineError::InvalidStateTransition { from, to })
                    if from == current.to_string() && to == target.to_string()
            ));
        }
        assert!(
            WorkItemRepository::new(storage.clone())
                .list("test_space", 100, None)
                .await
                .expect("queued work")
                .is_empty(),
            "budget refusal must preserve the original accepted attempt, never queue unbound work"
        );
        assert!(
            elephant_storage::CostLedgerDailyRepository::new(storage)
                .list("test_space", 100, None)
                .await
                .expect("cost ledger")
                .is_empty(),
            "mode admission must not reserve or reconcile another attempt's cost"
        );
    }

    #[tokio::test]
    async fn embedded_accepted_revision_rebuilds_same_mode_with_budget_and_legacy_guards() {
        accepted_revision_same_mode_contract(
            StorageClient::new_embedded().await.expect("embedded"),
        )
        .await;
    }

    #[tokio::test]
    async fn surreal_accepted_revision_rebuilds_same_mode_with_budget_and_legacy_guards() {
        accepted_revision_same_mode_contract(StorageClient::new_memory().await.expect("memory"))
            .await;
    }

    #[tokio::test]
    async fn test_execute_triage_source_queues_upgrade_on_downgrade() {
        let storage = StorageClient::new_memory().await.expect("storage");
        storage.query(SCHEMA).await.expect("schema");

        let source = make_source();
        let triage_plugin = GraphTriagePlugin::new();
        let budget = Budget {
            daily_euros_graph: 1.0,
            downgrade_policy: DowngradePolicy::DegradeModeWithQueue,
            ..Budget::default()
        };
        let budget_enforcer =
            BudgetEnforcer::new(storage.clone(), budget, "test_space".to_string());
        let decision_repo = IngestDecisionRepository::new(storage.clone());

        let result = execute_triage_source(
            &source,
            &triage_plugin,
            &budget_enforcer,
            &decision_repo,
            "test_space",
            "v1.0",
            "test_actor",
        )
        .await
        .expect("triage");

        assert!(result.was_downgraded);
        assert_eq!(result.decided_mode, IngestMode::Retrieval);

        let repo = WorkItemRepository::new(storage);
        let items = repo.list("test_space", 10, None).await.expect("work items");
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].kind, UPGRADE_INGEST_MODE_KIND);
        assert_eq!(items[0].status, WorkItemStatus::Runnable);
        assert_eq!(
            items[0].payload.get("target_mode").and_then(|v| v.as_str()),
            Some("graph")
        );
        assert_eq!(
            items[0]
                .payload
                .get("current_mode")
                .and_then(|v| v.as_str()),
            Some("retrieval")
        );
        assert!(
            items[0].run_after.is_some(),
            "Expected queued upgrade to have run_after"
        );
    }

    #[tokio::test]
    async fn test_execute_triage_source_rejects_per_item_cap() {
        let storage = StorageClient::new_memory().await.expect("storage");
        storage.query(SCHEMA).await.expect("schema");

        let source = make_source();
        let triage_plugin = CapBreachTriagePlugin::new();
        let budget = Budget {
            per_item_tokens: Some(10),
            ..Budget::default()
        };
        let budget_enforcer =
            BudgetEnforcer::new(storage.clone(), budget, "test_space".to_string());
        let decision_repo = IngestDecisionRepository::new(storage);

        let result = execute_triage_source(
            &source,
            &triage_plugin,
            &budget_enforcer,
            &decision_repo,
            "test_space",
            "v1.0",
            "test_actor",
        )
        .await;

        assert!(matches!(
            result,
            Err(PipelineError::PerItemCapExceeded { cap_type, .. }) if cap_type == "tokens"
        ));
    }

    #[tokio::test]
    async fn test_execute_triage_source_queues_on_concurrency_limit() {
        let storage = StorageClient::new_memory().await.expect("storage");
        storage.query(SCHEMA).await.expect("schema");

        let source = make_source();
        let triage_plugin = DefaultTriagePlugin::new();
        let mut budget = Budget::default();
        budget
            .concurrent_workers
            .insert("triage_source".to_string(), 1);
        let budget_enforcer =
            BudgetEnforcer::new(storage.clone(), budget, "test_space".to_string());
        let decision_repo = IngestDecisionRepository::new(storage.clone());

        let running_item = WorkItem {
            base: BaseFields::new(
                RecordId::new_v4(TableName::WorkItem),
                "test_space".to_string(),
                "worker".to_string(),
            ),
            kind: "triage_source".to_string(),
            payload: serde_json::json!({}),
            status: WorkItemStatus::Running,
            priority: 0,
            idempotency_key: "triage_source_running".to_string(),
            attempts: 0,
            lease_expires_at: None,
            leased_by: Some("worker".to_string()),
            run_after: None,
            last_error: None,
        };

        let repo = WorkItemRepository::new(storage.clone());
        repo.create(running_item).await.expect("running item");

        let result = execute_triage_source(
            &source,
            &triage_plugin,
            &budget_enforcer,
            &decision_repo,
            "test_space",
            "v1.0",
            "test_actor",
        )
        .await
        .expect("triage");

        assert_eq!(result.decided_mode, IngestMode::Metadata);
        assert_eq!(
            result.downgrade_reason.as_deref(),
            Some("queued:triage_source")
        );
        assert!(!result.was_downgraded);
    }

    #[tokio::test]
    async fn test_execute_ingest_metadata() {
        let storage = StorageClient::new_memory().await.expect("storage");
        let source = make_source();

        let result = execute_ingest_metadata(&source, &storage)
            .await
            .expect("metadata");

        assert_eq!(result.source_id, source.base.id);
        assert_eq!(
            result.metadata_artifact.source_id,
            source.base.id.to_string()
        );
        assert!(result.metadata_artifact.raw_metadata_patch.is_object());
    }

    #[tokio::test]
    async fn test_execute_upgrade_ingest_mode_allowed() {
        let storage = StorageClient::new_memory().await.expect("storage");
        storage.query(SCHEMA).await.expect("schema");
        let source = make_source();
        let budget = Budget::default();
        let budget_enforcer = BudgetEnforcer::new(storage, budget, "test_space".to_string());

        let result = execute_upgrade_ingest_mode(
            &source,
            IngestMode::Metadata,
            IngestMode::Retrieval,
            &budget_enforcer,
            1.0,
            "user_requested",
        )
        .await
        .expect("upgrade");

        assert!(result.allowed);
        assert_eq!(result.previous_mode, IngestMode::Metadata);
        assert_eq!(result.new_mode, IngestMode::Retrieval);
    }

    #[tokio::test]
    async fn test_execute_upgrade_ingest_mode_invalid() {
        let storage = StorageClient::new_memory().await.expect("storage");
        storage.query(SCHEMA).await.expect("schema");
        let source = make_source();
        let budget = Budget::default();
        let budget_enforcer = BudgetEnforcer::new(storage, budget, "test_space".to_string());

        let result = execute_upgrade_ingest_mode(
            &source,
            IngestMode::Graph,
            IngestMode::Retrieval, // Invalid: downgrade
            &budget_enforcer,
            1.0,
            "user_requested",
        )
        .await;

        assert!(result.is_err());
    }

    #[test]
    fn test_partial_artifact_data() {
        let data = serde_json::json!({"assertions": []});
        let partial = PartialArtifactData::new(data.clone(), "token_cap_exceeded");

        assert!(partial.partial);
        assert_eq!(
            partial.partial_reason,
            Some("token_cap_exceeded".to_string())
        );

        let artifact_data = partial.to_artifact_data();
        assert_eq!(artifact_data["partial"], true);
    }

    #[test]
    fn test_complete_artifact_data() {
        let data = serde_json::json!({"assertions": [1, 2, 3]});
        let complete = PartialArtifactData::complete(data.clone());

        assert!(!complete.partial);
        assert!(complete.partial_reason.is_none());
    }
}
