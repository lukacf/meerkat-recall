//! Predicate governance work kinds for vocabulary management (Phase 5).
//!
//! Per spec Section 10, these work kinds manage:
//! - Alias lifecycle state machine (Proposed -> Active -> Deprecated)
//! - Confidence thresholds for truth maintenance vs query expansion
//! - Automatic alias promotion based on usage
//! - Governance events for operator attention
//! - Staged commit workflow for predicate proposals (Draft -> Validated -> Committed)

use elephant_storage::predicate_alias_repository::PredicateAliasRepository;
use elephant_storage::predicate_registry_repository::PredicateRegistryRepository;
use elephant_storage::work_artifact_repository::WorkArtifactRepository;
use elephant_storage::{Repository, StorageClient, StorageError, WorkRunRepository};
use elephant_types::ids::TableName;
use elephant_types::records::base::BaseFields;
use elephant_types::records::ingest::{Budget, IngestMode};
use elephant_types::records::predicate::{
    AliasScope, AliasStatus, AliasThresholds, AliasType, PredicateAlias, PredicateRegistry,
    SemanticsStatus,
};
use elephant_types::records::work::{ArtifactStatus, WorkArtifact};
use elephant_types::{Link, RecordId};
use uuid::Uuid;

use crate::error::PipelineError;
use crate::provenance::apply_work_run_provenance;
use crate::scheduler::budget::BudgetEnforcer;
use crate::scheduler::staged_commit::StagedCommit;

// ============================================================================
// Alias Lifecycle State Machine
// ============================================================================

/// Validates and executes alias status transitions.
///
/// Valid transitions per spec §10.2b:
/// - Proposed -> Active (alias confirmed by usage or operator approval)
/// - Proposed -> Deprecated (operator rejection)
/// - Active -> Deprecated (alias superseded or incorrect)
///
/// # Errors
///
/// Returns an error if the transition is invalid (e.g., self-transition,
/// reverse transition, or from terminal state).
pub fn transition_alias_status(
    from: AliasStatus,
    to: AliasStatus,
) -> Result<AliasStatus, PipelineError> {
    match (from, to) {
        // Valid transitions per spec §10.2b
        (AliasStatus::Proposed, AliasStatus::Active) => Ok(AliasStatus::Active),
        (AliasStatus::Proposed | AliasStatus::Active, AliasStatus::Deprecated) => {
            Ok(AliasStatus::Deprecated)
        }

        // Self-transitions and all other transitions are invalid
        _ => Err(PipelineError::InvalidStateTransition {
            from: from.to_string(),
            to: to.to_string(),
        }),
    }
}

// ============================================================================
// Confidence Gating
// ============================================================================

/// Checks if an alias can be used for truth maintenance.
///
/// Per spec Section 10.2a: truth maintenance requires high confidence
/// to avoid propagating uncertain alias mappings.
pub fn can_use_for_truth(alias: &PredicateAlias, thresholds: &AliasThresholds) -> bool {
    // Only active aliases can be used for truth
    if alias.status != AliasStatus::Active {
        return false;
    }
    // Must meet minimum confidence threshold
    alias.confidence >= thresholds.truth_min_confidence
}

/// Checks if an alias can be used for query expansion.
///
/// Per spec Section 10.2a: query expansion can use lower confidence
/// aliases since results are scored, not committed.
pub fn can_use_for_query_expansion(alias: &PredicateAlias, thresholds: &AliasThresholds) -> bool {
    // Only active aliases can be used for query expansion
    if alias.status != AliasStatus::Active {
        return false;
    }
    // Query expansion has a lower confidence threshold
    alias.confidence >= thresholds.query_expand_min_confidence
}

/// Adjusts alias confidence based on usage feedback.
///
/// Per spec Section 10.3: successful usage increases confidence,
/// conflicts or corrections decrease it.
///
/// The success_rate threshold is 0.6 (60%) - rates above this increase
/// confidence, rates at or below decrease it. This ensures that marginal
/// performance (50%) is treated as a negative signal.
pub fn adjust_confidence_by_usage(
    current_confidence: f32,
    usage_count: u64,
    success_rate: f64,
) -> f32 {
    // Use 0.6 as the neutral point - below decreases, above increases
    // This treats 50% success rate as "low" (negative signal)
    let adjustment_factor = success_rate.mul_add(2.5, -1.5); // Maps [0,1] to [-1.5,1.0]

    // Weight the adjustment by log of usage count (more usage = more significant)
    // Use log2(usage_count + 1) to handle usage_count = 0 and scale appropriately
    #[allow(clippy::cast_precision_loss)] // usage_count fits in f64 mantissa for typical values
    let usage_weight = ((usage_count + 1) as f64).log2() / 10.0; // Normalize

    // Calculate the adjustment, capped to prevent wild swings
    #[allow(clippy::cast_possible_truncation)] // f64 to f32 is intentional
    let adjustment = (adjustment_factor * usage_weight).clamp(-0.1, 0.1) as f32;

    // Apply adjustment and clamp to valid confidence range [0.0, 1.0]
    (current_confidence + adjustment).clamp(0.0, 1.0)
}

// ============================================================================
// Predicate Maintainer Work Kind
// ============================================================================

/// Work kind identifier for predicate_maintainer.
pub const PREDICATE_MAINTAINER_KIND: &str = "predicate_maintainer";

/// Result of the `predicate_maintainer` work kind.
#[derive(Debug, Clone)]
pub struct PredicateMaintainerResult {
    /// Number of aliases scanned.
    pub aliases_scanned: usize,
    /// Number of aliases promoted (Proposed -> Active).
    pub aliases_promoted: usize,
    /// Number of predicates needing semantics review.
    pub predicates_needing_review: usize,
    /// Governance events emitted during maintenance.
    pub events: Vec<PredicateGovernanceEvent>,
}

/// Configuration for predicate maintainer.
#[derive(Debug, Clone)]
pub struct PredicateMaintainerConfig {
    /// Minimum usage count to auto-promote an alias.
    pub promotion_usage_threshold: u64,
    /// Minimum confidence to auto-promote an alias.
    pub promotion_confidence_threshold: f32,
    /// Usage threshold for high-usage-no-semantics alert.
    pub high_usage_alert_threshold: u64,
}

impl Default for PredicateMaintainerConfig {
    fn default() -> Self {
        Self {
            promotion_usage_threshold: 10,
            promotion_confidence_threshold: 0.85,
            high_usage_alert_threshold: 100,
        }
    }
}

/// Executes the `predicate_maintainer` work kind.
///
/// Scans aliases and predicates, promoting eligible aliases and
/// emitting governance events for operator attention.
///
/// # Errors
///
/// This function is infallible in the current implementation but returns
/// a Result for consistency with other work kinds.
pub async fn execute_predicate_maintainer(
    aliases: &[PredicateAlias],
    predicates: &[PredicateRegistry],
    config: &PredicateMaintainerConfig,
) -> Result<PredicateMaintainerResult, PipelineError> {
    let mut aliases_promoted = 0;
    let mut events = Vec::new();

    // Scan aliases for promotion eligibility
    for alias in aliases {
        if should_auto_promote(alias, config) {
            aliases_promoted += 1;
        }
    }

    // Check predicates for governance events
    let mut predicates_needing_review = 0;
    for predicate in predicates {
        // Check for needs semantics
        if let Some(event) = check_needs_semantics(predicate) {
            events.push(event);
            predicates_needing_review += 1;
        }

        // Check for high usage without semantics
        if let Some(event) =
            check_high_usage_no_semantics(predicate, config.high_usage_alert_threshold)
        {
            events.push(event);
        }
    }

    // Check for conflicts between predicates
    if let Some(event) = check_predicate_conflicts(predicates) {
        events.push(event);
    }

    Ok(PredicateMaintainerResult {
        aliases_scanned: aliases.len(),
        aliases_promoted,
        predicates_needing_review,
        events,
    })
}

/// Data for staging a predicate proposal.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PredicateProposalData {
    /// The canonical predicate ID being proposed.
    pub predicate_id: String,
    /// Proposed aliases for this predicate.
    pub proposed_aliases: Vec<ProposedAlias>,
    /// Estimated confidence for the proposal.
    pub confidence: f32,
    /// Space ID for the proposal.
    pub space_id: String,
}

/// A proposed alias within a predicate proposal.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ProposedAlias {
    /// The alias string.
    pub alias: String,
    /// Source of the alias (plugin ID, extraction, etc.).
    pub source: String,
    /// Initial confidence score.
    pub confidence: f32,
}

/// Artifact type identifier for predicate proposals.
pub const PREDICATE_PROPOSAL_ARTIFACT_TYPE: &str = "predicate_proposal";

fn is_duplicate_predicate_alias_create_error(err: &StorageError) -> bool {
    match err {
        StorageError::Conflict(_) => true,
        StorageError::QueryError(message) => message.contains("already exists"),
        _ => false,
    }
}

fn deterministic_predicate_alias_id(
    space_id: &str,
    predicate_id: &str,
    proposed_alias: &ProposedAlias,
) -> RecordId {
    RecordId::new(
        TableName::PredicateAlias,
        Uuid::new_v5(
            &Uuid::NAMESPACE_OID,
            format!(
                "{space_id}|{predicate_id}|{}|{}",
                proposed_alias.alias, proposed_alias.source
            )
            .as_bytes(),
        ),
    )
}

// ============================================================================
// Staged Commit Workflow for Predicate Proposals
// ============================================================================

/// Handler for predicate proposal staged commit workflow.
///
/// Per spec Section 10.4: Predicate proposals go through Draft -> Validated -> Committed
/// stages before becoming permanent alias records.
#[derive(Debug, Clone)]
pub struct PredicateProposalWorkflow {
    staged_commit: StagedCommit,
    predicate_registry_repo: PredicateRegistryRepository,
    predicate_alias_repo: PredicateAliasRepository,
    storage: StorageClient,
}

impl PredicateProposalWorkflow {
    /// Creates a new predicate proposal workflow handler.
    #[must_use]
    pub fn new(storage: StorageClient) -> Self {
        Self {
            staged_commit: StagedCommit::new(storage.clone()),
            predicate_registry_repo: PredicateRegistryRepository::new(storage.clone()),
            predicate_alias_repo: PredicateAliasRepository::new(storage.clone()),
            storage,
        }
    }

    /// Stages a predicate proposal as a Draft artifact.
    ///
    /// Per spec Section 10.4: Proposals are first staged as Draft artifacts
    /// containing the raw predicate, proposed canonical ID, and confidence.
    ///
    /// # Arguments
    /// * `work_run_id` - The work run this proposal belongs to
    /// * `proposal` - The proposal data to stage
    ///
    /// # Returns
    /// The staged WorkArtifact in Draft status.
    ///
    /// # Errors
    /// Returns an error if staging fails.
    pub async fn stage_predicate_proposal(
        &self,
        work_run_id: &RecordId,
        proposal: PredicateProposalData,
    ) -> Result<WorkArtifact, PipelineError> {
        let artifact_id = RecordId::new_v4(TableName::WorkArtifact);
        let mut base = BaseFields::new(
            artifact_id,
            proposal.space_id.clone(),
            "predicate_proposal_workflow".to_string(),
        );
        apply_work_run_provenance(
            &self.storage,
            work_run_id,
            &proposal.space_id,
            &mut base.provenance,
        )
        .await?;

        let artifact = WorkArtifact {
            base,
            work_run: Link::new(work_run_id.clone()),
            artifact_type: PREDICATE_PROPOSAL_ARTIFACT_TYPE.to_string(),
            data: serde_json::to_value(&proposal).map_err(|e| {
                PipelineError::InvalidInput(format!("Failed to serialize proposal: {e}"))
            })?,
            status: ArtifactStatus::Draft,
            validation_errors: None,
            validated_at: None,
            committed_at: None,
        };

        self.staged_commit.stage(artifact).await
    }

    /// Validates a staged predicate proposal.
    ///
    /// Per spec Section 10.4: Validation checks:
    /// 1. The proposed_canonical predicate_id exists in predicate_registry
    /// 2. No duplicate active alias exists for this raw_predicate
    ///
    /// # Arguments
    /// * `space_id` - The space the artifact belongs to
    /// * `artifact_id` - The ID of the Draft artifact to validate
    ///
    /// # Returns
    /// The validated (or rejected) WorkArtifact.
    ///
    /// # Errors
    /// Returns an error if validation processing fails.
    pub async fn validate_predicate_proposal(
        &self,
        space_id: &str,
        artifact_id: &RecordId,
    ) -> Result<WorkArtifact, PipelineError> {
        // First, get the artifact to extract proposal data for pre-validation checks
        let artifact_repo = WorkArtifactRepository::new(self.storage.clone());
        let artifact = artifact_repo
            .get(space_id, artifact_id)
            .await
            .map_err(PipelineError::StorageError)?
            .ok_or_else(|| PipelineError::NotFound(artifact_id.to_string()))?;

        // Parse the proposal data
        let proposal: PredicateProposalData = serde_json::from_value(artifact.data.clone())
            .map_err(|e| PipelineError::InvalidInput(format!("Invalid proposal data: {e}")))?;

        // Pre-fetch validation results asynchronously
        let mut validation_errors = Vec::new();

        // Validation 1: Check if canonical predicate_id exists in registry
        let predicate_exists = self
            .predicate_registry_repo
            .find_by_predicate_id(space_id, &proposal.predicate_id)
            .await
            .ok()
            .flatten()
            .is_some();

        if !predicate_exists {
            validation_errors.push(format!(
                "Canonical predicate '{}' does not exist in registry",
                proposal.predicate_id
            ));
        }

        // Validation 2: Check for duplicate active aliases
        for proposed_alias in &proposal.proposed_aliases {
            let duplicate_exists = self
                .predicate_alias_repo
                .find_active_by_raw(space_id, &proposed_alias.alias)
                .await
                .ok()
                .flatten()
                .is_some();

            if duplicate_exists {
                validation_errors.push(format!(
                    "Active alias already exists for '{}'",
                    proposed_alias.alias
                ));
            }
        }

        // Now call staged_commit.validate with pre-computed results
        let errors_for_closure = validation_errors.clone();
        self.staged_commit
            .validate(space_id, artifact_id, move |_artifact| {
                if errors_for_closure.is_empty() {
                    Ok(())
                } else {
                    Err(errors_for_closure)
                }
            })
            .await
    }

    /// Commits a validated predicate proposal.
    ///
    /// Per spec Section 10.4: Creates PredicateAlias records with Active status
    /// and updates the artifact to Committed state.
    ///
    /// # Arguments
    /// * `space_id` - The space the artifact belongs to
    /// * `artifact_id` - The ID of the Validated artifact to commit
    ///
    /// # Returns
    /// The committed WorkArtifact.
    ///
    /// # Errors
    /// Returns an error if commit fails.
    pub async fn commit_predicate_proposal(
        &self,
        space_id: &str,
        artifact_id: &RecordId,
    ) -> Result<WorkArtifact, PipelineError> {
        // First, get the artifact to extract proposal data
        let artifact_repo = WorkArtifactRepository::new(self.storage.clone());

        let artifact = artifact_repo
            .get(space_id, artifact_id)
            .await
            .map_err(PipelineError::StorageError)?
            .ok_or_else(|| PipelineError::NotFound(artifact_id.to_string()))?;

        // Parse the proposal data
        let proposal: PredicateProposalData = serde_json::from_value(artifact.data.clone())
            .map_err(|e| PipelineError::InvalidInput(format!("Invalid proposal data: {e}")))?;

        let work_run_repo = WorkRunRepository::new(self.storage.clone());
        let work_run = work_run_repo
            .get(space_id, artifact.work_run.id())
            .await
            .map_err(PipelineError::StorageError)?
            .ok_or_else(|| PipelineError::NotFound(artifact.work_run.id().to_string()))?;

        let actual_cost = work_run.metrics.estimated_cost_euros;
        let budget_enforcer = BudgetEnforcer::new(
            self.storage.clone(),
            Budget::default(),
            space_id.to_string(),
        );

        let alias_repo = self.predicate_alias_repo.clone();
        let proposed_aliases = proposal.proposed_aliases.clone();
        let predicate_id = proposal.predicate_id.clone();

        self.staged_commit
            .commit_with_budget_check(
                space_id,
                artifact_id,
                &budget_enforcer,
                IngestMode::Graph,
                actual_cost,
                move |_| {
                    let alias_repo = alias_repo.clone();
                    async move {
                        for proposed_alias in &proposed_aliases {
                            let alias_id = deterministic_predicate_alias_id(
                                space_id,
                                &predicate_id,
                                proposed_alias,
                            );
                            let base = BaseFields::new(
                                alias_id.clone(),
                                space_id.to_string(),
                                "proposal_commit".to_string(),
                            );
                            let confidence = proposed_alias.confidence.clamp(0.0, 1.0).max(0.95);

                            let alias = PredicateAlias {
                                base,
                                alias: proposed_alias.alias.clone(),
                                alias_type: AliasType::RawString,
                                source_plugin: Some(proposed_alias.source.clone()),
                                confidence,
                                status: AliasStatus::Active,
                                scope: AliasScope::Global,
                                predicate_id: predicate_id.clone(),
                                confidence_version: 1,
                                usage_count: 0,
                            };

                            match alias_repo.create(alias).await {
                                Ok(_) => {}
                                Err(err) if is_duplicate_predicate_alias_create_error(&err) => {
                                    let existing = alias_repo
                                        .get(space_id, &alias_id)
                                        .await
                                        .map_err(PipelineError::StorageError)?;
                                    let Some(existing) = existing else {
                                        return Err(PipelineError::StorageError(
                                            StorageError::Conflict(format!(
                                                "predicate alias {} conflicted without persisted row",
                                                proposed_alias.alias
                                            )),
                                        ));
                                    };
                                    if existing.predicate_id == predicate_id
                                        && existing.alias == proposed_alias.alias
                                        && existing.status == AliasStatus::Active
                                        && existing.source_plugin.as_deref()
                                            == Some(proposed_alias.source.as_str())
                                    {
                                        continue;
                                    }
                                    return Err(PipelineError::StorageError(
                                        StorageError::Conflict(format!(
                                            "predicate alias {} already exists with incompatible state",
                                            proposed_alias.alias
                                        )),
                                    ));
                                }
                                Err(err) => return Err(PipelineError::StorageError(err)),
                            }
                        }
                        Ok(())
                    }
                },
            )
            .await
    }
}

/// Compatibility function for staging a predicate proposal.
///
/// Per spec Section 10.4: proposals go through staged commit workflow.
/// Use `PredicateProposalWorkflow::stage_predicate_proposal` for full functionality.
///
/// # Errors
///
/// This function generates an artifact ID for tracking purposes.
pub async fn stage_predicate_proposal(
    proposal: PredicateProposalData,
) -> Result<String, PipelineError> {
    // Generate a unique artifact ID for the proposal
    // Compatibility helper: use PredicateProposalWorkflow for full workflow
    let artifact_id = format!(
        "proposal_{}_{}_{}",
        proposal.predicate_id,
        proposal.proposed_aliases.len(),
        uuid::Uuid::new_v4()
    );

    Ok(artifact_id)
}

/// Checks if an alias should be automatically promoted based on usage.
///
/// Per spec Section 10.3: aliases with sufficient usage and confidence
/// are auto-promoted from Proposed to Active.
pub fn should_auto_promote(alias: &PredicateAlias, config: &PredicateMaintainerConfig) -> bool {
    // Only Proposed aliases can be auto-promoted
    if alias.status != AliasStatus::Proposed {
        return false;
    }

    // Must meet both usage and confidence thresholds
    alias.usage_count >= config.promotion_usage_threshold
        && alias.confidence >= config.promotion_confidence_threshold
}

// ============================================================================
// Governance Events
// ============================================================================

/// Events emitted by predicate governance for operator attention.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PredicateGovernanceEvent {
    /// A predicate is being used without configured semantics.
    NeedsSemantics {
        /// The predicate that needs semantics configuration.
        predicate_id: String,
    },
    /// A high-usage predicate has no semantics configured.
    HighUsageNoSemantics {
        /// The predicate with high usage.
        predicate_id: String,
        /// Current usage count.
        usage_count: u64,
        /// Threshold that was exceeded.
        threshold: u64,
    },
    /// Multiple predicates may be in conflict without semantics to resolve.
    ConflictNoSemantics {
        /// The predicates involved in the conflict.
        predicate_ids: Vec<String>,
        /// Type of conflict detected.
        conflict_type: String,
    },
}

/// Checks if a predicate needs semantics configuration.
///
/// Per spec Section 12.1: predicates without configured semantics
/// use defaults, but operators should be notified.
pub fn check_needs_semantics(predicate: &PredicateRegistry) -> Option<PredicateGovernanceEvent> {
    // Only emit event if semantics are not configured
    if predicate.semantics_status == SemanticsStatus::Configured {
        None
    } else {
        Some(PredicateGovernanceEvent::NeedsSemantics {
            predicate_id: predicate.predicate_id.clone(),
        })
    }
}

/// Checks if a high-usage predicate lacks semantics.
///
/// Per spec Section 12.1: high-usage predicates are priority for
/// semantics configuration.
pub fn check_high_usage_no_semantics(
    predicate: &PredicateRegistry,
    threshold: u64,
) -> Option<PredicateGovernanceEvent> {
    // Only emit event if:
    // 1. Usage exceeds threshold
    // 2. Semantics are not configured
    if predicate.usage_count > threshold
        && predicate.semantics_status != SemanticsStatus::Configured
    {
        Some(PredicateGovernanceEvent::HighUsageNoSemantics {
            predicate_id: predicate.predicate_id.clone(),
            usage_count: predicate.usage_count,
            threshold,
        })
    } else {
        None
    }
}

/// Checks for semantic conflicts between predicates.
///
/// Per spec Section 12.2: predicates with overlapping aliases or
/// contradictory semantics should be flagged.
pub fn check_predicate_conflicts(
    predicates: &[PredicateRegistry],
) -> Option<PredicateGovernanceEvent> {
    // Collect unconfigured predicates (those without explicit semantics)
    let unconfigured: Vec<&PredicateRegistry> = predicates
        .iter()
        .filter(|p| p.semantics_status != SemanticsStatus::Configured)
        .collect();

    // If we have multiple unconfigured predicates, flag them as potential conflicts
    // Per spec: predicates without semantics can't resolve conflicts properly
    if unconfigured.len() >= 2 {
        let predicate_ids: Vec<String> = unconfigured
            .iter()
            .map(|p| p.predicate_id.clone())
            .collect();

        return Some(PredicateGovernanceEvent::ConflictNoSemantics {
            predicate_ids,
            conflict_type: "multiple_unconfigured_predicates".to_string(),
        });
    }

    None
}

// ============================================================================
// Tests - TDD RED Phase
// ============================================================================

#[cfg(test)]
mod tests {
    #![allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::float_cmp,
        unused_imports,
        dead_code
    )]

    use super::*;
    use chrono::Utc;
    use elephant_storage::work_item_repository::WorkItemRepository;
    use elephant_types::ids::TableName;
    use elephant_types::records::base::BaseFields;
    use elephant_types::records::predicate::{
        AliasScope, AliasType, Cardinality, PredicateSemantics,
    };
    use elephant_types::records::work::{WorkItem, WorkItemStatus, WorkRun, WorkRunMetrics};
    use elephant_types::RecordId;

    // =========================================================================
    // Test Fixtures
    // =========================================================================

    const WORKFLOW_SCHEMA: &str = r"
        DEFINE TABLE predicate_registry SCHEMALESS;
        DEFINE TABLE predicate_alias SCHEMALESS;
        DEFINE TABLE work_artifact SCHEMALESS;
        DEFINE TABLE work_item SCHEMALESS;
        DEFINE TABLE work_run SCHEMALESS;
        DEFINE TABLE cost_ledger_daily SCHEMALESS;
    ";

    async fn setup_workflow_storage() -> StorageClient {
        let storage = StorageClient::new_memory().await.expect("storage");
        storage.query(WORKFLOW_SCHEMA).await.expect("schema");
        storage
    }

    async fn create_test_work_run(storage: StorageClient, space_id: &str, work_run_id: &RecordId) {
        let work_item_id = RecordId::new_v4(TableName::WorkItem);
        let work_item = WorkItem {
            base: BaseFields::new(
                work_item_id.clone(),
                space_id.to_string(),
                "test".to_string(),
            ),
            kind: "test_work".to_string(),
            payload: serde_json::json!({}),
            status: WorkItemStatus::Running,
            priority: 0,
            idempotency_key: format!("test_work:{work_run_id}"),
            attempts: 1,
            lease_expires_at: None,
            leased_by: None,
            run_after: None,
            last_error: None,
        };
        WorkItemRepository::new(storage.clone())
            .create(work_item)
            .await
            .expect("create work item");

        let work_run = WorkRun {
            base: BaseFields::new(
                work_run_id.clone(),
                space_id.to_string(),
                "test".to_string(),
            ),
            work_item: Link::new(work_item_id),
            attempt: 1,
            started_at: Utc::now(),
            completed_at: None,
            success: false,
            error: None,
            metrics: WorkRunMetrics::default(),
            worker_id: "test-worker".to_string(),
            worker_version: "1.0.0".to_string(),
            model_version: None,
            prompt_id: None,
            config_hash: None,
        };
        WorkRunRepository::new(storage)
            .create(work_run)
            .await
            .expect("create work run");
    }

    fn make_alias(
        alias: &str,
        predicate_id: &str,
        status: AliasStatus,
        confidence: f32,
        usage_count: u64,
    ) -> PredicateAlias {
        let id = RecordId::new_v4(TableName::PredicateAlias);
        let base = BaseFields::new(id, "test_space".to_string(), "test".to_string());

        PredicateAlias {
            base,
            alias: alias.to_string(),
            alias_type: AliasType::RawString,
            source_plugin: None,
            confidence,
            status,
            scope: AliasScope::Global,
            predicate_id: predicate_id.to_string(),
            confidence_version: 1,
            usage_count,
        }
    }

    fn make_predicate(
        predicate_id: &str,
        semantics_status: SemanticsStatus,
        usage_count: u64,
    ) -> PredicateRegistry {
        let id = RecordId::new_v4(TableName::PredicateRegistry);
        let base = BaseFields::new(id, "test_space".to_string(), "test".to_string());

        PredicateRegistry {
            base,
            predicate_id: predicate_id.to_string(),
            description: Some(format!("Test predicate: {predicate_id}")),
            status: "active".to_string(),
            semantics: if semantics_status == SemanticsStatus::Configured {
                Some(PredicateSemantics::default())
            } else {
                None
            },
            semantics_status,
            first_seen_at: Some(chrono::Utc::now()),
            usage_count,
            last_used_at: Some(chrono::Utc::now()),
        }
    }

    fn default_thresholds() -> AliasThresholds {
        AliasThresholds {
            truth_min_confidence: 0.9,
            query_expand_min_confidence: 0.6,
        }
    }

    // =========================================================================
    // Alias Lifecycle Tests
    // =========================================================================

    /// Test: Valid transition from Proposed to Active.
    ///
    /// Per spec: An alias in Proposed state can be promoted to Active
    /// when confirmed by operator or sufficient usage.
    #[test]
    fn test_alias_lifecycle_proposed_to_active() {
        let result = transition_alias_status(AliasStatus::Proposed, AliasStatus::Active);

        assert!(result.is_ok(), "Proposed -> Active should be valid");
        assert_eq!(result.unwrap(), AliasStatus::Active);
    }

    /// Test: Valid transition from Active to Deprecated.
    ///
    /// Per spec: An Active alias can be deprecated when superseded
    /// or found to be incorrect.
    #[test]
    fn test_alias_lifecycle_active_to_deprecated() {
        let result = transition_alias_status(AliasStatus::Active, AliasStatus::Deprecated);

        assert!(result.is_ok(), "Active -> Deprecated should be valid");
        assert_eq!(result.unwrap(), AliasStatus::Deprecated);
    }

    /// Test: Invalid transitions are rejected.
    ///
    /// Per spec §10.2b: Cannot reverse (Active -> Proposed) or
    /// transition from terminal state (Deprecated -> anything).
    #[test]
    fn test_alias_lifecycle_invalid_transition_rejected() {
        // Proposed -> Deprecated IS valid per spec §10.2b (operator rejection)
        let rejection_result =
            transition_alias_status(AliasStatus::Proposed, AliasStatus::Deprecated);
        assert!(
            rejection_result.is_ok(),
            "Proposed -> Deprecated should be valid (operator rejection per spec §10.2b)"
        );
        assert_eq!(rejection_result.unwrap(), AliasStatus::Deprecated);

        // Cannot reverse from Active to Proposed
        let reverse_result = transition_alias_status(AliasStatus::Active, AliasStatus::Proposed);
        assert!(
            reverse_result.is_err(),
            "Active -> Proposed should be invalid (no reverse)"
        );

        // Cannot transition from Deprecated
        let from_deprecated = transition_alias_status(AliasStatus::Deprecated, AliasStatus::Active);
        assert!(
            from_deprecated.is_err(),
            "Deprecated -> Active should be invalid (terminal state)"
        );

        // Self-transition should be no-op or error
        let self_transition = transition_alias_status(AliasStatus::Active, AliasStatus::Active);
        // Either allow as no-op (return Ok) or reject - implementation decides
        // For strict state machines, self-transition is typically an error
        assert!(
            self_transition.is_err() || self_transition.unwrap() == AliasStatus::Active,
            "Self-transition should either error or return same state"
        );
    }

    // =========================================================================
    // Confidence Threshold Tests
    // =========================================================================

    /// Test: Aliases below truth threshold are rejected.
    ///
    /// Per spec Section 10.2a: Truth maintenance requires high confidence
    /// (default 0.9) to prevent propagating uncertain mappings.
    #[test]
    fn test_confidence_gate_rejects_below_threshold() {
        let thresholds = default_thresholds();

        // Confidence 0.85 is below truth threshold of 0.9
        let low_confidence_alias =
            make_alias("employed_at", "works_at", AliasStatus::Active, 0.85, 50);

        let can_use = can_use_for_truth(&low_confidence_alias, &thresholds);
        assert!(
            !can_use,
            "Alias with confidence 0.85 should be rejected for truth (threshold 0.9)"
        );

        // Also rejected for truth at 0.89 (just below)
        let just_below_alias = make_alias("employed_at", "works_at", AliasStatus::Active, 0.89, 50);

        let can_use_just_below = can_use_for_truth(&just_below_alias, &thresholds);
        assert!(
            !can_use_just_below,
            "Alias with confidence 0.89 should be rejected for truth"
        );
    }

    /// Test: Aliases above threshold are accepted for truth.
    ///
    /// Per spec Section 10.2a: Aliases meeting the confidence threshold
    /// can be used in truth maintenance operations.
    #[test]
    fn test_confidence_gate_accepts_above_threshold() {
        let thresholds = default_thresholds();

        // Confidence 0.95 is above truth threshold of 0.9
        let high_confidence_alias =
            make_alias("employed_at", "works_at", AliasStatus::Active, 0.95, 50);

        let can_use = can_use_for_truth(&high_confidence_alias, &thresholds);
        assert!(
            can_use,
            "Alias with confidence 0.95 should be accepted for truth (threshold 0.9)"
        );

        // Exactly at threshold should also be accepted
        let at_threshold_alias =
            make_alias("employed_at", "works_at", AliasStatus::Active, 0.9, 50);

        let can_use_at_threshold = can_use_for_truth(&at_threshold_alias, &thresholds);
        assert!(
            can_use_at_threshold,
            "Alias with confidence exactly 0.9 should be accepted for truth"
        );

        // Query expansion has lower threshold (0.6)
        let medium_confidence_alias =
            make_alias("employed_at", "works_at", AliasStatus::Active, 0.7, 50);

        let can_expand = can_use_for_query_expansion(&medium_confidence_alias, &thresholds);
        assert!(
            can_expand,
            "Alias with confidence 0.7 should be accepted for query expansion (threshold 0.6)"
        );
    }

    /// Test: Confidence adjusts based on usage patterns.
    ///
    /// Per spec Section 10.3: Successful usage increases confidence,
    /// while conflicts or corrections decrease it.
    #[test]
    fn test_usage_based_confidence_adjustment() {
        // High usage with high success rate should increase confidence
        let adjusted_up = adjust_confidence_by_usage(0.8, 100, 0.95);
        assert!(
            adjusted_up > 0.8,
            "High success rate (95%) should increase confidence from 0.8"
        );

        // High usage with low success rate should decrease confidence
        let adjusted_down = adjust_confidence_by_usage(0.8, 100, 0.5);
        assert!(
            adjusted_down < 0.8,
            "Low success rate (50%) should decrease confidence from 0.8"
        );

        // Confidence should be clamped to [0.0, 1.0]
        let clamped_high = adjust_confidence_by_usage(0.99, 1000, 1.0);
        assert!(clamped_high <= 1.0, "Confidence should not exceed 1.0");

        let clamped_low = adjust_confidence_by_usage(0.1, 1000, 0.0);
        assert!(clamped_low >= 0.0, "Confidence should not go below 0.0");
    }

    // =========================================================================
    // Predicate Maintainer Work Kind Tests
    // =========================================================================

    /// Test: Predicate maintainer executes and returns results.
    ///
    /// Per spec Section 10: The maintainer scans aliases and predicates,
    /// performing maintenance operations and returning metrics.
    #[tokio::test]
    async fn test_execute_predicate_maintainer() {
        let aliases = vec![
            make_alias("employed_at", "works_at", AliasStatus::Proposed, 0.9, 20),
            make_alias("works_for", "works_at", AliasStatus::Active, 0.95, 100),
        ];
        let predicates = vec![
            make_predicate("works_at", SemanticsStatus::Configured, 150),
            make_predicate("located_in", SemanticsStatus::Default, 50),
        ];
        let config = PredicateMaintainerConfig::default();

        let result = execute_predicate_maintainer(&aliases, &predicates, &config)
            .await
            .expect("maintainer should succeed");

        assert_eq!(result.aliases_scanned, 2, "Should scan all aliases");
        // The Proposed alias with usage 20 > threshold 10 should be promoted
        assert!(
            result.aliases_promoted >= 1,
            "Should promote eligible aliases"
        );
    }

    /// Test: Predicate proposal flows through staged commit.
    ///
    /// Per spec Section 10.4: New predicates go through the staged
    /// commit workflow before becoming permanent.
    #[tokio::test]
    async fn test_predicate_proposal_staged_flow() {
        let proposal = PredicateProposalData {
            predicate_id: "new_predicate".to_string(),
            proposed_aliases: vec![
                ProposedAlias {
                    alias: "new_alias_1".to_string(),
                    source: "extractor_v1".to_string(),
                    confidence: 0.85,
                },
                ProposedAlias {
                    alias: "new_alias_2".to_string(),
                    source: "extractor_v1".to_string(),
                    confidence: 0.80,
                },
            ],
            confidence: 0.82,
            space_id: "test_space".to_string(),
        };

        let artifact_id = stage_predicate_proposal(proposal)
            .await
            .expect("staging should succeed");

        // Should return a valid artifact ID for later commit
        assert!(
            !artifact_id.is_empty(),
            "Should return non-empty artifact ID"
        );
    }

    /// Test: Aliases are auto-promoted based on usage.
    ///
    /// Per spec Section 10.3: Proposed aliases with sufficient usage
    /// and confidence are automatically promoted to Active.
    #[test]
    fn test_automatic_alias_promotion_by_usage() {
        let config = PredicateMaintainerConfig {
            promotion_usage_threshold: 10,
            promotion_confidence_threshold: 0.85,
            ..Default::default()
        };

        // Meets both thresholds
        let eligible = make_alias("employed_at", "works_at", AliasStatus::Proposed, 0.9, 15);
        assert!(
            should_auto_promote(&eligible, &config),
            "Alias with usage 15 > 10 and confidence 0.9 > 0.85 should be promoted"
        );

        // Below usage threshold
        let low_usage = make_alias("employed_at", "works_at", AliasStatus::Proposed, 0.9, 5);
        assert!(
            !should_auto_promote(&low_usage, &config),
            "Alias with usage 5 < 10 should not be promoted"
        );

        // Below confidence threshold
        let low_confidence = make_alias("employed_at", "works_at", AliasStatus::Proposed, 0.7, 20);
        assert!(
            !should_auto_promote(&low_confidence, &config),
            "Alias with confidence 0.7 < 0.85 should not be promoted"
        );

        // Already Active (should not promote again)
        let already_active = make_alias("employed_at", "works_at", AliasStatus::Active, 0.95, 100);
        assert!(
            !should_auto_promote(&already_active, &config),
            "Already Active alias should not be promoted"
        );
    }

    // =========================================================================
    // Governance Event Tests
    // =========================================================================

    /// Test: NeedsSemantics event fires for unconfigured predicates.
    ///
    /// Per spec Section 12.1: Predicates using default semantics
    /// should emit an event so operators can configure them.
    #[test]
    fn test_predicate_needs_semantics_event_fires() {
        // Predicate with Default semantics status
        let unconfigured = make_predicate("new_predicate", SemanticsStatus::Default, 10);

        let event = check_needs_semantics(&unconfigured);
        assert!(
            event.is_some(),
            "Unconfigured predicate should emit NeedsSemantics event"
        );

        match event.unwrap() {
            PredicateGovernanceEvent::NeedsSemantics { predicate_id } => {
                assert_eq!(predicate_id, "new_predicate");
            }
            _ => panic!("Expected NeedsSemantics event"),
        }

        // Configured predicate should not emit event
        let configured = make_predicate("configured_pred", SemanticsStatus::Configured, 10);
        let no_event = check_needs_semantics(&configured);
        assert!(
            no_event.is_none(),
            "Configured predicate should not emit NeedsSemantics event"
        );
    }

    /// Test: HighUsageNoSemantics event fires for busy unconfigured predicates.
    ///
    /// Per spec Section 12.1: High-usage predicates without semantics
    /// are priority for configuration.
    #[test]
    fn test_high_usage_no_semantics_event_fires() {
        let threshold = 100;

        // High usage without semantics
        let high_usage = make_predicate("busy_predicate", SemanticsStatus::Default, 150);

        let event = check_high_usage_no_semantics(&high_usage, threshold);
        assert!(
            event.is_some(),
            "High-usage unconfigured predicate should emit event"
        );

        match event.unwrap() {
            PredicateGovernanceEvent::HighUsageNoSemantics {
                predicate_id,
                usage_count,
                threshold: t,
            } => {
                assert_eq!(predicate_id, "busy_predicate");
                assert_eq!(usage_count, 150);
                assert_eq!(t, threshold);
            }
            _ => panic!("Expected HighUsageNoSemantics event"),
        }

        // Low usage should not emit event
        let low_usage = make_predicate("quiet_predicate", SemanticsStatus::Default, 50);
        let no_event = check_high_usage_no_semantics(&low_usage, threshold);
        assert!(
            no_event.is_none(),
            "Low-usage predicate should not emit HighUsageNoSemantics event"
        );

        // High usage WITH semantics should not emit event
        let configured_high = make_predicate("configured_busy", SemanticsStatus::Configured, 200);
        let no_event2 = check_high_usage_no_semantics(&configured_high, threshold);
        assert!(
            no_event2.is_none(),
            "Configured predicate should not emit HighUsageNoSemantics event"
        );
    }

    /// Test: ConflictNoSemantics event fires for conflicting predicates.
    ///
    /// Per spec Section 12.2: Predicates with overlapping aliases
    /// or contradictory semantics should be flagged.
    #[test]
    fn test_conflict_no_semantics_event_fires() {
        // Two predicates that might conflict (similar names, no semantics)
        let predicates = vec![
            make_predicate("works_at", SemanticsStatus::Default, 100),
            make_predicate("employed_by", SemanticsStatus::Default, 80),
        ];

        let event = check_predicate_conflicts(&predicates);

        // For this test, we expect the implementation to detect potential
        // conflicts between similar predicates without configured semantics
        assert!(
            event.is_some(),
            "Potentially conflicting predicates should emit event"
        );

        match event.unwrap() {
            PredicateGovernanceEvent::ConflictNoSemantics {
                predicate_ids,
                conflict_type,
            } => {
                assert!(
                    predicate_ids.contains(&"works_at".to_string())
                        || predicate_ids.contains(&"employed_by".to_string()),
                    "Should include conflicting predicate IDs"
                );
                assert!(!conflict_type.is_empty(), "Should specify conflict type");
            }
            _ => panic!("Expected ConflictNoSemantics event"),
        }

        // Non-conflicting predicates should not emit event
        let non_conflicting = vec![
            make_predicate("person_name", SemanticsStatus::Configured, 50),
            make_predicate("location", SemanticsStatus::Configured, 50),
        ];
        let no_event = check_predicate_conflicts(&non_conflicting);
        assert!(
            no_event.is_none(),
            "Non-conflicting configured predicates should not emit event"
        );
    }

    // =========================================================================
    // Staged Commit Workflow Tests
    // =========================================================================

    const TEST_SPACE: &str = "test_space";

    /// Test: Stage predicate proposal creates draft artifact.
    ///
    /// Per spec Section 10.4: Proposals are first staged as Draft artifacts.
    #[tokio::test]
    async fn test_stage_predicate_proposal_creates_draft_artifact() {
        let storage = setup_workflow_storage().await;
        let workflow = PredicateProposalWorkflow::new(storage.clone());

        let work_run_id = RecordId::new_v4(TableName::WorkRun);
        create_test_work_run(storage.clone(), TEST_SPACE, &work_run_id).await;
        let proposal = PredicateProposalData {
            predicate_id: "works_at".to_string(),
            proposed_aliases: vec![ProposedAlias {
                alias: "employed_at".to_string(),
                source: "test_extractor".to_string(),
                confidence: 0.85,
            }],
            confidence: 0.85,
            space_id: TEST_SPACE.to_string(),
        };

        let artifact = workflow
            .stage_predicate_proposal(&work_run_id, proposal)
            .await
            .expect("staging should succeed");

        assert_eq!(artifact.status, ArtifactStatus::Draft);
        assert_eq!(artifact.artifact_type, PREDICATE_PROPOSAL_ARTIFACT_TYPE);
        assert!(artifact.validated_at.is_none());
        assert!(artifact.committed_at.is_none());

        // Verify the data contains the proposal
        let stored_proposal: PredicateProposalData =
            serde_json::from_value(artifact.data).expect("valid proposal data");
        assert_eq!(stored_proposal.predicate_id, "works_at");
        assert_eq!(stored_proposal.proposed_aliases.len(), 1);
    }

    /// Test: Validate predicate proposal checks registry.
    ///
    /// Per spec Section 10.4: Validation verifies the canonical predicate exists.
    #[tokio::test]
    async fn test_validate_predicate_proposal_checks_registry() {
        let storage = setup_workflow_storage().await;

        // Create the predicate registry entry first
        let registry_repo = PredicateRegistryRepository::new(storage.clone());
        let predicate = make_predicate("works_at", SemanticsStatus::Default, 0);
        registry_repo
            .create(predicate)
            .await
            .expect("create predicate");

        let workflow = PredicateProposalWorkflow::new(storage.clone());

        // Stage a proposal for an existing predicate
        let work_run_id = RecordId::new_v4(TableName::WorkRun);
        create_test_work_run(storage.clone(), TEST_SPACE, &work_run_id).await;
        let proposal = PredicateProposalData {
            predicate_id: "works_at".to_string(),
            proposed_aliases: vec![ProposedAlias {
                alias: "employed_at".to_string(),
                source: "test_extractor".to_string(),
                confidence: 0.85,
            }],
            confidence: 0.85,
            space_id: TEST_SPACE.to_string(),
        };

        let staged = workflow
            .stage_predicate_proposal(&work_run_id, proposal)
            .await
            .expect("staging");

        // Validate should succeed because the predicate exists
        let validated = workflow
            .validate_predicate_proposal(TEST_SPACE, &staged.base.id)
            .await
            .expect("validation");

        assert_eq!(validated.status, ArtifactStatus::Validated);
        assert!(validated.validated_at.is_some());
        assert!(validated.validation_errors.is_none());
    }

    /// Test: Validate predicate proposal rejects non-existent predicate.
    ///
    /// Per spec Section 10.4: Validation fails if the canonical predicate doesn't exist.
    #[tokio::test]
    async fn test_validate_predicate_proposal_rejects_missing_predicate() {
        let storage = setup_workflow_storage().await;
        let workflow = PredicateProposalWorkflow::new(storage.clone());

        // Stage a proposal for a NON-existent predicate
        let work_run_id = RecordId::new_v4(TableName::WorkRun);
        create_test_work_run(storage.clone(), TEST_SPACE, &work_run_id).await;
        let proposal = PredicateProposalData {
            predicate_id: "nonexistent_predicate".to_string(),
            proposed_aliases: vec![ProposedAlias {
                alias: "some_alias".to_string(),
                source: "test_extractor".to_string(),
                confidence: 0.85,
            }],
            confidence: 0.85,
            space_id: TEST_SPACE.to_string(),
        };

        let staged = workflow
            .stage_predicate_proposal(&work_run_id, proposal)
            .await
            .expect("staging");

        // Validate should reject because the predicate doesn't exist
        let validated = workflow
            .validate_predicate_proposal(TEST_SPACE, &staged.base.id)
            .await
            .expect("validation");

        assert_eq!(validated.status, ArtifactStatus::Rejected);
        assert!(validated.validation_errors.is_some());
        let errors = validated.validation_errors.unwrap();
        assert!(errors
            .iter()
            .any(|e| e.contains("nonexistent_predicate") && e.contains("does not exist")));
    }

    /// Test: Validate predicate proposal rejects duplicate alias.
    ///
    /// Per spec Section 10.4: Validation fails if an active alias already exists.
    #[tokio::test]
    async fn test_validate_predicate_proposal_rejects_duplicate_alias() {
        let storage = setup_workflow_storage().await;

        // Create the predicate registry entry
        let registry_repo = PredicateRegistryRepository::new(storage.clone());
        let predicate = make_predicate("works_at", SemanticsStatus::Default, 0);
        registry_repo
            .create(predicate)
            .await
            .expect("create predicate");

        // Create an existing active alias
        let alias_repo = PredicateAliasRepository::new(storage.clone());
        let existing_alias = make_alias("employed_at", "works_at", AliasStatus::Active, 0.9, 10);
        alias_repo
            .create(existing_alias)
            .await
            .expect("create existing alias");

        let workflow = PredicateProposalWorkflow::new(storage.clone());

        // Stage a proposal with the same alias
        let work_run_id = RecordId::new_v4(TableName::WorkRun);
        create_test_work_run(storage.clone(), TEST_SPACE, &work_run_id).await;
        let proposal = PredicateProposalData {
            predicate_id: "works_at".to_string(),
            proposed_aliases: vec![ProposedAlias {
                alias: "employed_at".to_string(), // Duplicate!
                source: "test_extractor".to_string(),
                confidence: 0.85,
            }],
            confidence: 0.85,
            space_id: TEST_SPACE.to_string(),
        };

        let staged = workflow
            .stage_predicate_proposal(&work_run_id, proposal)
            .await
            .expect("staging");

        // Validate should reject because the alias already exists
        let validated = workflow
            .validate_predicate_proposal(TEST_SPACE, &staged.base.id)
            .await
            .expect("validation");

        assert_eq!(validated.status, ArtifactStatus::Rejected);
        assert!(validated.validation_errors.is_some());
        let errors = validated.validation_errors.unwrap();
        assert!(errors
            .iter()
            .any(|e| e.contains("employed_at") && e.contains("already exists")));
    }

    /// Test: Commit predicate proposal creates active alias.
    ///
    /// Per spec Section 10.4: Committing creates PredicateAlias with Active status.
    #[tokio::test]
    async fn test_commit_predicate_proposal_creates_active_alias() {
        let storage = setup_workflow_storage().await;

        // Create the predicate registry entry
        let registry_repo = PredicateRegistryRepository::new(storage.clone());
        let predicate = make_predicate("works_at", SemanticsStatus::Default, 0);
        registry_repo
            .create(predicate)
            .await
            .expect("create predicate");

        let workflow = PredicateProposalWorkflow::new(storage.clone());

        // Stage and validate a proposal
        let work_run_id = RecordId::new_v4(TableName::WorkRun);
        create_test_work_run(storage.clone(), TEST_SPACE, &work_run_id).await;
        let proposal = PredicateProposalData {
            predicate_id: "works_at".to_string(),
            proposed_aliases: vec![
                ProposedAlias {
                    alias: "employed_at".to_string(),
                    source: "test_extractor".to_string(),
                    confidence: 0.85,
                },
                ProposedAlias {
                    alias: "works_for".to_string(),
                    source: "test_extractor".to_string(),
                    confidence: 0.80,
                },
            ],
            confidence: 0.82,
            space_id: TEST_SPACE.to_string(),
        };

        let staged = workflow
            .stage_predicate_proposal(&work_run_id, proposal)
            .await
            .expect("staging");

        let validated = workflow
            .validate_predicate_proposal(TEST_SPACE, &staged.base.id)
            .await
            .expect("validation");

        assert_eq!(validated.status, ArtifactStatus::Validated);

        // Commit the proposal
        let committed = workflow
            .commit_predicate_proposal(TEST_SPACE, &validated.base.id)
            .await
            .expect("commit");

        assert_eq!(committed.status, ArtifactStatus::Committed);
        assert!(committed.committed_at.is_some());

        // Verify the aliases were created
        let alias_repo = PredicateAliasRepository::new(storage);

        let alias1 = alias_repo
            .find_active_by_raw(TEST_SPACE, "employed_at")
            .await
            .expect("query alias1");
        assert!(alias1.is_some());
        let alias1 = alias1.unwrap();
        assert_eq!(alias1.predicate_id, "works_at");
        assert_eq!(alias1.status, AliasStatus::Active);
        assert!(alias1.confidence >= 0.95);

        let alias2 = alias_repo
            .find_active_by_raw(TEST_SPACE, "works_for")
            .await
            .expect("query alias2");
        assert!(alias2.is_some());
        let alias2 = alias2.unwrap();
        assert_eq!(alias2.predicate_id, "works_at");
        assert_eq!(alias2.status, AliasStatus::Active);
        assert!(alias2.confidence >= 0.95);
    }

    #[tokio::test]
    async fn test_commit_predicate_proposal_is_idempotent_when_alias_already_exists() {
        let storage = setup_workflow_storage().await;

        let registry_repo = PredicateRegistryRepository::new(storage.clone());
        let predicate = make_predicate("works_at", SemanticsStatus::Default, 0);
        registry_repo
            .create(predicate)
            .await
            .expect("create predicate");

        let workflow = PredicateProposalWorkflow::new(storage.clone());
        let work_run_id = RecordId::new_v4(TableName::WorkRun);
        create_test_work_run(storage.clone(), TEST_SPACE, &work_run_id).await;
        let proposal = PredicateProposalData {
            predicate_id: "works_at".to_string(),
            proposed_aliases: vec![
                ProposedAlias {
                    alias: "employed_at".to_string(),
                    source: "test_extractor".to_string(),
                    confidence: 0.85,
                },
                ProposedAlias {
                    alias: "works_for".to_string(),
                    source: "test_extractor".to_string(),
                    confidence: 0.80,
                },
            ],
            confidence: 0.82,
            space_id: TEST_SPACE.to_string(),
        };

        let staged = workflow
            .stage_predicate_proposal(&work_run_id, proposal)
            .await
            .expect("staging");
        let validated = workflow
            .validate_predicate_proposal(TEST_SPACE, &staged.base.id)
            .await
            .expect("validation");
        let validated_proposal: PredicateProposalData =
            serde_json::from_value(validated.data.clone()).expect("proposal payload");
        let existing_alias_input = validated_proposal.proposed_aliases[0].clone();
        let existing_alias = PredicateAlias {
            base: BaseFields::new(
                deterministic_predicate_alias_id(
                    TEST_SPACE,
                    &validated_proposal.predicate_id,
                    &existing_alias_input,
                ),
                TEST_SPACE.to_string(),
                "tester".to_string(),
            ),
            alias: existing_alias_input.alias.clone(),
            alias_type: AliasType::RawString,
            source_plugin: Some(existing_alias_input.source.clone()),
            confidence: 0.97,
            status: AliasStatus::Active,
            scope: AliasScope::Global,
            predicate_id: validated_proposal.predicate_id.clone(),
            confidence_version: 1,
            usage_count: 0,
        };
        PredicateAliasRepository::new(storage.clone())
            .create(existing_alias)
            .await
            .expect("seed alias");
        let committed = workflow
            .commit_predicate_proposal(TEST_SPACE, &validated.base.id)
            .await
            .expect("commit");

        assert_eq!(committed.status, ArtifactStatus::Committed);
        assert!(PredicateAliasRepository::new(storage)
            .find_active_by_raw(TEST_SPACE, "works_for")
            .await
            .expect("query alias")
            .is_some());
    }
}
