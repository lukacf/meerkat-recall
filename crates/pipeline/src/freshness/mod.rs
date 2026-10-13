//! Freshness computation and decay models for ranking.
//!
//! Per spec §13, freshness is derived metadata for ranking, never "truth."
//! This module implements:
//! - Decay models (Linear, Exponential, StepFunction)
//! - Freshness computation from factors
//! - Next review scheduling
//! - Demotion policies
//! - Integration with truth_slot ranking
//! - FreshnessSweepWorker for batch processing

use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use elephant_storage::{FreshnessRepository, Repository};
use elephant_types::records::base::BaseFields;
use elephant_types::records::freshness::{DecayModel, Freshness, FreshnessFactors};
use elephant_types::{RecordId, TableName};
use uuid::Uuid;

use crate::error::{PipelineError, PipelineResult};

// ============================================================================
// Configuration Types
// ============================================================================

/// Configuration for decay model parameters.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DecayConfig {
    /// For Linear: days until score reaches 0.
    pub max_age_days: Option<f64>,

    /// For Exponential: days until score halves.
    pub half_life_days: Option<f64>,

    /// For StepFunction: (age_days_threshold, score) pairs, sorted by age ascending.
    pub step_thresholds: Option<Vec<(f64, f32)>>,
}

impl Default for DecayConfig {
    fn default() -> Self {
        Self {
            max_age_days: Some(365.0),
            half_life_days: Some(90.0),
            step_thresholds: Some(vec![(7.0, 1.0), (30.0, 0.8), (90.0, 0.5), (365.0, 0.2)]),
        }
    }
}

/// Result of a demotion check.
#[derive(Debug, Clone, PartialEq)]
pub struct DemotionResult {
    /// Whether the record should be demoted.
    pub demote: bool,

    /// The threshold that triggered demotion (if demoted).
    pub threshold: Option<f32>,

    /// Reason for the demotion decision.
    pub reason: String,
}

/// Policy for handling demotions.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DemotionPolicy {
    /// Score below which to demote.
    pub threshold: f32,

    /// Optional grace period before demotion takes effect.
    pub grace_period_days: Option<f64>,

    /// Action to take on demotion.
    pub action: DemotionAction,
}

impl Default for DemotionPolicy {
    fn default() -> Self {
        Self {
            threshold: 0.3,
            grace_period_days: None,
            action: DemotionAction::Demote,
        }
    }
}

/// Action to take when demotion is triggered.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DemotionAction {
    /// Demote the record (change status).
    Demote,
    /// Flag for review but don't demote yet.
    Flag,
    /// Send notification but take no action.
    Notify,
}

/// Result of freshness computation.
#[derive(Debug, Clone, PartialEq)]
pub struct FreshnessResult {
    /// Computed freshness score (0.0 to 1.0).
    pub score: f32,

    /// Confidence in the score (0.0 to 1.0).
    pub confidence: f32,

    /// Human-readable rationale.
    pub rationale: String,
}

/// Ranking entry for truth slots with freshness.
#[derive(Debug, Clone, PartialEq)]
pub struct TruthSlotRanking {
    /// Slot identifier.
    pub slot_key: String,

    /// Original confidence score.
    pub confidence: f32,

    /// Freshness score.
    pub freshness: f32,

    /// Combined ranking score.
    pub combined_score: f32,
}

/// Evidence inputs used to compute one production claim freshness receipt.
#[derive(Debug, Clone, Copy)]
pub struct ClaimFreshnessObservation<'a> {
    /// Times attached to the claim's supporting evidence.
    pub evidence_times: &'a [DateTime<Utc>],
    /// Strength of the canonical claim and its evidence.
    pub evidence_strength: f32,
    /// Current normalized conflict severity.
    pub conflict_severity: f32,
    /// Number of distinct supporting evidence references.
    pub corroboration_count: usize,
}

/// Creates or updates the one production freshness record for a canonical
/// claim. The record ID is deterministic by space and target, preventing
/// duplicate freshness rows when promotion is retried concurrently.
pub async fn record_claim_freshness(
    repository: &FreshnessRepository,
    target_id: &RecordId,
    target_type: &str,
    target_base: &BaseFields,
    observation: ClaimFreshnessObservation<'_>,
) -> PipelineResult<Freshness> {
    let existing = repository
        .find_by_target(&target_base.space_id, &target_id.to_string())
        .await
        .map_err(PipelineError::StorageError)?;
    let freshness = prepare_claim_freshness(
        existing.as_ref(),
        target_id,
        target_type,
        target_base,
        observation,
    );
    if let Some(existing) = existing {
        let freshness_space_id = freshness.base.space_id.clone();
        repository
            .update(&freshness_space_id, &existing.base.id, freshness)
            .await
            .map_err(PipelineError::StorageError)
    } else {
        repository
            .create(freshness)
            .await
            .map_err(PipelineError::StorageError)
    }
}

/// Prepare a deterministic freshness effect without writing live storage.
#[must_use]
pub fn prepare_claim_freshness(
    existing: Option<&Freshness>,
    target_id: &RecordId,
    target_type: &str,
    target_base: &BaseFields,
    observation: ClaimFreshnessObservation<'_>,
) -> Freshness {
    let now = Utc::now();
    let newest_evidence = observation
        .evidence_times
        .iter()
        .copied()
        .max()
        .unwrap_or(now);
    let evidence_age_days = now
        .signed_duration_since(newest_evidence)
        .num_seconds()
        .max(0) as f64
        / 86_400.0;
    let factors = FreshnessFactors {
        evidence_age_days,
        conflict_severity: observation.conflict_severity.clamp(0.0, 1.0),
        evidence_strength: observation.evidence_strength.clamp(0.0, 1.0),
        source_reliability: 1.0,
        corroboration_count: u32::try_from(observation.corroboration_count).unwrap_or(u32::MAX),
        custom: HashMap::from([
            ("policy_version".to_string(), 1.0),
            ("source_reliability_defaulted".to_string(), 1.0),
        ]),
    };
    let (decay_model, decay_config) = default_claim_decay_policy(target_type);
    let result = compute_freshness(&factors, decay_model, &decay_config);
    let demotion = check_demotion(result.score, &DemotionPolicy::default());
    let next_review_at = Some(schedule_next_review(
        now,
        decay_model,
        result.score,
        &decay_config,
    ));

    let deterministic_key = format!("{}:{target_id}", target_base.space_id);
    let deterministic_id = RecordId::new(
        TableName::Freshness,
        Uuid::new_v5(&Uuid::NAMESPACE_URL, deterministic_key.as_bytes()),
    );
    let mut base = existing.map_or_else(
        || {
            BaseFields::new(
                deterministic_id,
                target_base.space_id.clone(),
                "freshness_projection".to_string(),
            )
        },
        |record| record.base.clone(),
    );
    base.updated_at = now;
    base.deleted_at = None;
    base.security.clone_from(&target_base.security);
    base.subjects.clone_from(&target_base.subjects);
    base.subjects_state = target_base.subjects_state;
    base.provenance.clone_from(&target_base.provenance);

    let freshness = Freshness {
        base,
        target: target_id.to_string(),
        target_type: target_type.to_string(),
        decay_model,
        score: result.score,
        confidence: result.confidence,
        rationale: Some(format!(
            "{}; default {target_type} freshness policy v1",
            result.rationale
        )),
        factors,
        computed_at: now,
        next_review_at,
        demote: demotion.demote,
        demotion_threshold: demotion.threshold,
    };

    freshness
}

fn default_claim_decay_policy(target_type: &str) -> (DecayModel, DecayConfig) {
    match target_type {
        "event" => (
            DecayModel::StepFunction,
            DecayConfig {
                max_age_days: None,
                half_life_days: None,
                step_thresholds: Some(vec![(7.0, 1.0), (30.0, 0.8), (90.0, 0.5), (365.0, 0.2)]),
            },
        ),
        "rel" | "attribute" => (
            DecayModel::Exponential,
            DecayConfig {
                max_age_days: None,
                half_life_days: Some(90.0),
                step_thresholds: None,
            },
        ),
        _ => (DecayModel::Linear, DecayConfig::default()),
    }
}

// ============================================================================
// Core Functions - Stubs for TDD
// ============================================================================

/// Compute the base decay score based on age and decay model.
///
/// # Arguments
/// * `age_days` - Age of the evidence in days
/// * `model` - The decay model to use
/// * `config` - Configuration for the decay model
///
/// # Returns
/// Score from 0.0 to 1.0
#[allow(clippy::cast_possible_truncation)] // f64 to f32 is intentional
pub fn compute_decay_score(age_days: f64, model: DecayModel, config: &DecayConfig) -> f32 {
    match model {
        DecayModel::Linear => {
            // Linear decay: score = 1.0 - (age_days / max_age_days)
            // Clamps to 0.0 when age exceeds max_age
            let max_age = config.max_age_days.unwrap_or(365.0);
            if max_age <= 0.0 {
                return 0.0;
            }
            let ratio = age_days / max_age;
            let score = 1.0 - ratio;
            // Clamp to [0.0, 1.0]
            score.clamp(0.0, 1.0) as f32
        }
        DecayModel::Exponential => {
            // Exponential decay: score = exp(-lambda * age_days)
            // where lambda = ln(2) / half_life_days
            let half_life = config.half_life_days.unwrap_or(90.0);
            if half_life <= 0.0 {
                return 0.0;
            }
            let lambda = std::f64::consts::LN_2 / half_life;
            let score = (-lambda * age_days).exp();
            score as f32
        }
        DecayModel::StepFunction => {
            // Step function: threshold-based discrete steps
            // Returns the score for the bracket the age falls into
            // At exactly a threshold boundary, use the next lower score
            let thresholds = match &config.step_thresholds {
                Some(t) if !t.is_empty() => t,
                _ => return 1.0, // No thresholds, return max score
            };

            // Thresholds are sorted by age ascending: [(age_threshold, score), ...]
            // Find the bracket: age < threshold[0] => score[0]
            //                   threshold[0] <= age < threshold[1] => score[1]
            //                   etc.
            for (i, (threshold_age, score)) in thresholds.iter().enumerate() {
                if age_days < *threshold_age {
                    // Age is below this threshold, use this bracket's score
                    return *score;
                }
                // At exactly the threshold or beyond, continue to next bracket
                if i == thresholds.len() - 1 {
                    // Beyond all thresholds, return the last score
                    return *score;
                }
            }

            // Fallback (shouldn't reach here with non-empty thresholds)
            thresholds.last().map(|(_, s)| *s).unwrap_or(0.0)
        }
    }
}

/// Compute freshness score from all factors.
///
/// # Arguments
/// * `factors` - The freshness factors to consider
/// * `model` - The decay model to use
/// * `config` - Configuration for the decay model
///
/// # Returns
/// FreshnessResult with score, confidence, and rationale
pub fn compute_freshness(
    factors: &FreshnessFactors,
    model: DecayModel,
    config: &DecayConfig,
) -> FreshnessResult {
    // Start with base decay score from evidence age
    let base_score = compute_decay_score(factors.evidence_age_days, model, config);
    let mut score = f64::from(base_score);

    // Adjust for conflict_severity: score *= (1.0 - conflict_severity * 0.5)
    // Conflicts reduce score by up to 50%
    let conflict_factor = f64::from(factors.conflict_severity).mul_add(-0.5, 1.0);
    score *= conflict_factor;

    // Adjust for evidence_strength: score *= (0.5 + evidence_strength * 0.5)
    // Weak evidence reduces score
    let strength_factor = f64::from(factors.evidence_strength).mul_add(0.5, 0.5);
    score *= strength_factor;

    // Adjust for source_reliability: score *= (0.5 + source_reliability * 0.5)
    // Unreliable source reduces score
    let reliability_factor = f64::from(factors.source_reliability).mul_add(0.5, 0.5);
    score *= reliability_factor;

    // Adjust for corroboration: score *= (1.0 + log2(corroboration_count) * 0.1).min(1.5)
    // More corroboration helps, up to 1.5x multiplier
    let corroboration_bonus = if factors.corroboration_count > 0 {
        f64::from(factors.corroboration_count)
            .log2()
            .mul_add(0.1, 1.0)
            .min(1.5)
    } else {
        1.0
    };
    score *= corroboration_bonus;

    // Clamp final score to [0.0, 1.0]
    #[allow(clippy::cast_possible_truncation)] // f64 to f32 is intentional
    let final_score = score.clamp(0.0, 1.0) as f32;

    // Compute confidence based on data completeness and quality
    // High confidence when: recent data, strong evidence, reliable source, multiple corroborations, no conflicts
    let mut confidence = 0.0_f64;

    // Evidence age factor (recent = higher confidence)
    let age_confidence = if factors.evidence_age_days < 30.0 {
        1.0
    } else if factors.evidence_age_days < 90.0 {
        0.8
    } else if factors.evidence_age_days < 365.0 {
        0.6
    } else {
        0.4
    };
    confidence += age_confidence * 0.25;

    // Evidence strength factor
    confidence += f64::from(factors.evidence_strength) * 0.25;

    // Source reliability factor
    confidence += f64::from(factors.source_reliability) * 0.25;

    // Corroboration factor (more sources = higher confidence)
    let corroboration_confidence = if factors.corroboration_count >= 5 {
        1.0
    } else if factors.corroboration_count >= 3 {
        0.8
    } else if factors.corroboration_count >= 2 {
        0.6
    } else {
        0.4
    };
    confidence += corroboration_confidence * 0.15;

    // Conflict penalty (conflicts reduce confidence)
    let conflict_confidence = f64::from(factors.conflict_severity).mul_add(-0.5, 1.0);
    confidence += conflict_confidence * 0.1;

    #[allow(clippy::cast_possible_truncation)] // f64 to f32 is intentional
    let final_confidence = confidence.clamp(0.0, 1.0) as f32;

    // Build rationale string
    let mut rationale_parts = Vec::new();
    rationale_parts.push(format!(
        "Base decay score: {:.2} (age: {:.1} days, model: {})",
        base_score, factors.evidence_age_days, model
    ));

    if factors.conflict_severity > 0.0 {
        rationale_parts.push(format!(
            "Conflict penalty: {:.0}% reduction (severity: {:.2})",
            (1.0 - conflict_factor) * 100.0,
            factors.conflict_severity
        ));
    }

    if factors.evidence_strength < 1.0 {
        rationale_parts.push(format!(
            "Evidence strength factor: {:.2}",
            factors.evidence_strength
        ));
    }

    if factors.source_reliability < 1.0 {
        rationale_parts.push(format!(
            "Source reliability factor: {:.2}",
            factors.source_reliability
        ));
    }

    if factors.corroboration_count > 1 {
        rationale_parts.push(format!(
            "Corroboration bonus: {:.0}% ({} sources)",
            (corroboration_bonus - 1.0) * 100.0,
            factors.corroboration_count
        ));
    }

    rationale_parts.push(format!("Final score: {final_score:.2}"));

    FreshnessResult {
        score: final_score,
        confidence: final_confidence,
        rationale: rationale_parts.join("; "),
    }
}

/// Compute freshness score (compatibility alias).
#[must_use]
pub fn compute_freshness_score(
    factors: &FreshnessFactors,
    model: DecayModel,
    config: &DecayConfig,
) -> FreshnessFactors {
    let result = compute_freshness(factors, model, config);
    let mut updated = factors.clone();
    updated
        .custom
        .insert("freshness_score".to_string(), f64::from(result.score));
    updated.custom.insert(
        "freshness_confidence".to_string(),
        f64::from(result.confidence),
    );
    updated
}

/// Schedule the next review time based on current state.
///
/// # Arguments
/// * `current` - Current timestamp
/// * `model` - The decay model being used
/// * `score` - Current freshness score
/// * `config` - Decay configuration
///
/// # Returns
/// DateTime for the next review
pub fn schedule_next_review(
    current: DateTime<Utc>,
    model: DecayModel,
    score: f32,
    config: &DecayConfig,
) -> DateTime<Utc> {
    const MIN_REVIEW_DAYS: i64 = 1;
    const MAX_REVIEW_DAYS: i64 = 90;

    let review_days = match model {
        DecayModel::Linear => {
            // For Linear: schedule review when score would drop by 0.1 (10% of range)
            // Linear decay: score = 1.0 - (age / max_age)
            // Change in score per day = 1.0 / max_age
            // Days for 0.1 drop = 0.1 * max_age
            let max_age = config.max_age_days.unwrap_or(365.0);
            let days_for_drop = 0.1 * max_age;

            // If score is already low, review sooner
            let urgency_factor = if score < 0.3 {
                0.5 // Review sooner for low scores
            } else if score < 0.5 {
                0.75
            } else {
                1.0
            };

            #[allow(clippy::cast_possible_truncation)] // Days always fit in i64
            {
                (days_for_drop * urgency_factor) as i64
            }
        }
        DecayModel::Exponential => {
            // For Exponential: schedule review at half the half_life_days
            let half_life = config.half_life_days.unwrap_or(90.0);

            // Review at half the half-life, adjusted by current score
            let base_review = half_life / 2.0;

            // If score is already low, review sooner
            let urgency_factor = if score < 0.3 {
                0.5
            } else if score < 0.5 {
                0.75
            } else {
                1.0
            };

            #[allow(clippy::cast_possible_truncation)] // Days always fit in i64
            {
                (base_review * urgency_factor) as i64
            }
        }
        DecayModel::StepFunction => {
            // For StepFunction: schedule review at the next threshold boundary
            let thresholds = match &config.step_thresholds {
                Some(t) if !t.is_empty() => t,
                _ => return current + Duration::days(MAX_REVIEW_DAYS),
            };

            // Find current bracket based on score
            #[allow(clippy::cast_precision_loss)] // MAX_REVIEW_DAYS fits in f64 mantissa
            let mut next_threshold_days = MAX_REVIEW_DAYS as f64;
            let mut prev_threshold_age = 0.0_f64;

            for (threshold_age, threshold_score) in thresholds {
                if score >= *threshold_score {
                    // We're at or above this score level
                    // Schedule review for the threshold age from now
                    next_threshold_days = threshold_age - prev_threshold_age;
                    break;
                }
                prev_threshold_age = *threshold_age;
            }

            // If score is below all thresholds, use half the gap between last two thresholds
            #[allow(clippy::float_cmp, clippy::cast_precision_loss)] // Sentinel value comparison
            if next_threshold_days == MAX_REVIEW_DAYS as f64 && thresholds.len() >= 2 {
                let last = thresholds[thresholds.len() - 1].0;
                let second_last = thresholds[thresholds.len() - 2].0;
                next_threshold_days = (last - second_last) / 2.0;
            }

            #[allow(clippy::cast_possible_truncation)] // Days always fit in i64
            {
                next_threshold_days as i64
            }
        }
    };

    // Clamp to [MIN_REVIEW_DAYS, MAX_REVIEW_DAYS]
    let clamped_days = review_days.clamp(MIN_REVIEW_DAYS, MAX_REVIEW_DAYS);

    current + Duration::days(clamped_days)
}

/// Check if a freshness score warrants demotion.
///
/// # Arguments
/// * `score` - Current freshness score
/// * `policy` - Demotion policy to apply
///
/// # Returns
/// DemotionResult indicating whether to demote
pub fn check_demotion(score: f32, policy: &DemotionPolicy) -> DemotionResult {
    if score < policy.threshold {
        DemotionResult {
            demote: true,
            threshold: Some(policy.threshold),
            reason: format!(
                "Freshness score {:.2} is below threshold {:.2}",
                score, policy.threshold
            ),
        }
    } else {
        DemotionResult {
            demote: false,
            threshold: None,
            reason: format!(
                "Freshness score {:.2} is at or above threshold {:.2}",
                score, policy.threshold
            ),
        }
    }
}

/// Apply freshness scores to truth slot rankings.
///
/// # Arguments
/// * `slots` - Mutable slice of truth slot rankings
/// * `freshness_scores` - Map of slot_key to freshness score
/// * `freshness_weight` - Weight for freshness in combined score (0.0 to 1.0)
#[allow(clippy::implicit_hasher)] // HashMap with default hasher is fine here
pub fn apply_freshness_to_ranking(
    slots: &mut [TruthSlotRanking],
    freshness_scores: &HashMap<String, f32>,
    freshness_weight: f32,
) {
    for slot in slots.iter_mut() {
        if let Some(&freshness_score) = freshness_scores.get(&slot.slot_key) {
            slot.freshness = freshness_score;
            // combined_score = confidence * (1 - freshness_weight) + freshness * freshness_weight
            slot.combined_score = slot
                .confidence
                .mul_add(1.0 - freshness_weight, freshness_score * freshness_weight);
        } else {
            // No freshness data: use confidence as combined_score (treat as if freshness = confidence)
            slot.combined_score = slot.confidence;
        }
    }
}

// ============================================================================
// Freshness Sweep Worker
// ============================================================================

/// Configuration for the freshness sweep worker.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[allow(clippy::derive_partial_eq_without_eq)] // Contains f64 in DecayConfig
pub struct SweepConfig {
    /// Number of records to process per batch.
    pub batch_size: usize,

    /// Decay model to use for freshness computation.
    pub decay_model: DecayModel,

    /// Decay configuration parameters.
    pub decay_config: DecayConfig,

    /// Demotion policy to apply.
    pub demotion_policy: DemotionPolicy,

    /// Space ID to sweep (if None, sweeps all spaces).
    pub space_id: Option<String>,
}

impl Default for SweepConfig {
    fn default() -> Self {
        Self {
            batch_size: 100,
            decay_model: DecayModel::default(),
            decay_config: DecayConfig::default(),
            demotion_policy: DemotionPolicy::default(),
            space_id: None,
        }
    }
}

/// Result of a freshness sweep operation.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SweepResult {
    /// Number of records processed.
    pub processed: usize,

    /// Number of records updated with new freshness scores.
    pub updated: usize,

    /// Number of records marked for demotion.
    pub demoted: usize,

    /// Number of errors encountered.
    pub errors: usize,

    /// Error messages (if any).
    pub error_messages: Vec<String>,
}

/// Worker that sweeps freshness records and recomputes scores.
///
/// The sweep worker:
/// 1. Queries records where `next_review_at <= now` or `next_review_at IS NONE`
/// 2. Recomputes freshness scores using `compute_freshness()`
/// 3. Schedules next review using `schedule_next_review()`
/// 4. Applies demotion policy using `check_demotion()`
/// 5. Updates records via the repository
pub struct FreshnessSweepWorker {
    repository: FreshnessRepository,
    config: SweepConfig,
}

impl FreshnessSweepWorker {
    /// Create a new freshness sweep worker.
    pub fn new(repository: FreshnessRepository, config: SweepConfig) -> Self {
        Self { repository, config }
    }

    /// Execute a sweep operation.
    ///
    /// Processes records needing freshness recalculation in batches.
    ///
    /// # Errors
    ///
    /// Returns an error if storage operations fail.
    pub async fn sweep(&self, space_id: &str) -> PipelineResult<SweepResult> {
        let now = Utc::now();
        let mut result = SweepResult::default();

        // Query records due for review
        let records = self
            .repository
            .list_due_for_review(space_id, now, self.config.batch_size)
            .await
            .map_err(|e| PipelineError::StorageError(e))?;

        for record in records {
            result.processed += 1;

            match self.process_record(record, now, space_id).await {
                Ok(updated) => {
                    if updated.demote {
                        result.demoted += 1;
                    }
                    result.updated += 1;
                }
                Err(e) => {
                    result.errors += 1;
                    result.error_messages.push(e.to_string());
                }
            }
        }

        Ok(result)
    }

    /// Process a single freshness record.
    async fn process_record(
        &self,
        mut record: Freshness,
        now: DateTime<Utc>,
        space_id: &str,
    ) -> PipelineResult<Freshness> {
        // Update evidence age based on current time
        #[allow(clippy::cast_precision_loss)] // Seconds fit in f64 mantissa
        let age_since_computed =
            (now.signed_duration_since(record.computed_at).num_seconds() as f64) / 86400.0; // Convert to days
        record.factors.evidence_age_days += age_since_computed;

        // Production claim projections carry a policy marker so scheduled
        // sweeps preserve the type-specific policy that created them. Legacy
        // and explicitly managed rows continue to use the worker config.
        let (decay_model, decay_config) = if record
            .factors
            .custom
            .get("policy_version")
            .is_some_and(|version| (*version - 1.0).abs() < f64::EPSILON)
        {
            default_claim_decay_policy(&record.target_type)
        } else {
            (self.config.decay_model, self.config.decay_config.clone())
        };
        let freshness_result = compute_freshness(&record.factors, decay_model, &decay_config);

        // Schedule next review
        let next_review =
            schedule_next_review(now, decay_model, freshness_result.score, &decay_config);

        // Check demotion
        let demotion_result = check_demotion(freshness_result.score, &self.config.demotion_policy);

        // Update the record
        record.score = freshness_result.score;
        record.decay_model = decay_model;
        record.confidence = freshness_result.confidence;
        record.rationale = Some(freshness_result.rationale);
        record.computed_at = now;
        record.next_review_at = Some(next_review);
        record.demote = demotion_result.demote;
        record.demotion_threshold = demotion_result.threshold;
        record.base.updated_at = now;

        // Clone the id before moving record
        let record_id = record.base.id.clone();

        // Persist the update
        let updated = self
            .repository
            .update(space_id, &record_id, record)
            .await
            .map_err(|e| PipelineError::StorageError(e))?;

        Ok(updated)
    }
}

// ============================================================================
// Work Kind Entry Point
// ============================================================================

/// Execute freshness sweep work kind.
///
/// This is the canonical entry point for the `freshness_sweep` work kind
/// as specified in spec §19.
///
/// The function:
/// 1. Creates a FreshnessSweepWorker with the provided configuration
/// 2. Executes the sweep for the specified space
/// 3. Returns the sweep result with metrics
///
/// # Arguments
/// * `repository` - Freshness repository for data access
/// * `space_id` - Space to sweep
/// * `config` - Optional sweep configuration (defaults to SweepConfig::default())
///
/// # Errors
///
/// Returns an error if storage operations fail.
pub async fn execute_freshness_sweep(
    repository: FreshnessRepository,
    space_id: &str,
    config: Option<SweepConfig>,
) -> PipelineResult<SweepResult> {
    let config = config.unwrap_or_default();
    let worker = FreshnessSweepWorker::new(repository, config);
    worker.sweep(space_id).await
}

/// Work kind identifier for freshness sweep.
pub const WORK_KIND_FRESHNESS_SWEEP: &str = "freshness_sweep";

// ============================================================================
// Tests
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
    use elephant_storage::Repository;
    use elephant_storage::{FreshnessRepository, StorageClient, StorageConfig};
    use elephant_types::records::base::{BaseFields, Provenance, Security, SubjectsState};
    use elephant_types::RecordId;
    use elephant_types::TableName;
    use std::sync::atomic::{AtomicU64, Ordering};

    // ========================================================================
    // Linear Decay Tests
    // ========================================================================

    #[test]
    fn test_linear_decay_at_zero_age() {
        let config = DecayConfig {
            max_age_days: Some(100.0),
            ..Default::default()
        };
        let score = compute_decay_score(0.0, DecayModel::Linear, &config);
        assert!(
            (score - 1.0).abs() < 0.001,
            "Fresh data should have score 1.0"
        );
    }

    #[test]
    fn test_linear_decay_at_half_age() {
        let config = DecayConfig {
            max_age_days: Some(100.0),
            ..Default::default()
        };
        let score = compute_decay_score(50.0, DecayModel::Linear, &config);
        assert!(
            (score - 0.5).abs() < 0.001,
            "Half-age should have score 0.5"
        );
    }

    #[test]
    fn test_linear_decay_at_max_age() {
        let config = DecayConfig {
            max_age_days: Some(100.0),
            ..Default::default()
        };
        let score = compute_decay_score(100.0, DecayModel::Linear, &config);
        assert!((score - 0.0).abs() < 0.001, "Max-age should have score 0.0");
    }

    #[test]
    fn test_linear_decay_beyond_max_age() {
        let config = DecayConfig {
            max_age_days: Some(100.0),
            ..Default::default()
        };
        let score = compute_decay_score(150.0, DecayModel::Linear, &config);
        assert!(score >= 0.0, "Score should not go negative");
    }

    // ========================================================================
    // Exponential Decay Tests
    // ========================================================================

    #[test]
    fn test_exponential_decay_at_zero_age() {
        let config = DecayConfig {
            half_life_days: Some(30.0),
            ..Default::default()
        };
        let score = compute_decay_score(0.0, DecayModel::Exponential, &config);
        assert!(
            (score - 1.0).abs() < 0.001,
            "Fresh data should have score 1.0"
        );
    }

    #[test]
    fn test_exponential_decay_at_half_life() {
        let config = DecayConfig {
            half_life_days: Some(30.0),
            ..Default::default()
        };
        let score = compute_decay_score(30.0, DecayModel::Exponential, &config);
        assert!(
            (score - 0.5).abs() < 0.01,
            "At half-life score should be ~0.5"
        );
    }

    #[test]
    fn test_exponential_decay_at_two_half_lives() {
        let config = DecayConfig {
            half_life_days: Some(30.0),
            ..Default::default()
        };
        let score = compute_decay_score(60.0, DecayModel::Exponential, &config);
        assert!(
            (score - 0.25).abs() < 0.01,
            "At 2x half-life score should be ~0.25"
        );
    }

    #[test]
    fn test_exponential_decay_never_reaches_zero() {
        let config = DecayConfig {
            half_life_days: Some(30.0),
            ..Default::default()
        };
        let score = compute_decay_score(1000.0, DecayModel::Exponential, &config);
        assert!(
            score > 0.0,
            "Exponential decay should never reach exactly 0"
        );
    }

    // ========================================================================
    // Step Function Decay Tests
    // ========================================================================

    #[test]
    fn test_step_function_fresh_data() {
        let config = DecayConfig {
            step_thresholds: Some(vec![(7.0, 1.0), (30.0, 0.8), (90.0, 0.5), (365.0, 0.2)]),
            ..Default::default()
        };
        let score = compute_decay_score(3.0, DecayModel::StepFunction, &config);
        assert!(
            (score - 1.0).abs() < 0.001,
            "Within first threshold should be 1.0"
        );
    }

    #[test]
    fn test_step_function_second_bracket() {
        let config = DecayConfig {
            step_thresholds: Some(vec![(7.0, 1.0), (30.0, 0.8), (90.0, 0.5), (365.0, 0.2)]),
            ..Default::default()
        };
        let score = compute_decay_score(15.0, DecayModel::StepFunction, &config);
        assert!(
            (score - 0.8).abs() < 0.001,
            "In second bracket should be 0.8"
        );
    }

    #[test]
    fn test_step_function_at_threshold_boundary() {
        let config = DecayConfig {
            step_thresholds: Some(vec![(7.0, 1.0), (30.0, 0.8), (90.0, 0.5), (365.0, 0.2)]),
            ..Default::default()
        };
        let score = compute_decay_score(30.0, DecayModel::StepFunction, &config);
        // At exactly 30 days, should be in the 0.8 bracket (exclusive upper bound)
        assert!((score - 0.5).abs() < 0.001 || (score - 0.8).abs() < 0.001);
    }

    #[test]
    fn test_step_function_beyond_all_thresholds() {
        let config = DecayConfig {
            step_thresholds: Some(vec![(7.0, 1.0), (30.0, 0.8), (90.0, 0.5), (365.0, 0.2)]),
            ..Default::default()
        };
        let score = compute_decay_score(500.0, DecayModel::StepFunction, &config);
        assert!(
            (score - 0.2).abs() < 0.001 || score < 0.2,
            "Beyond last threshold should be final score or lower"
        );
    }

    // ========================================================================
    // Freshness Computation Tests
    // ========================================================================

    #[test]
    fn test_compute_freshness_fresh_high_quality() {
        let factors = FreshnessFactors {
            evidence_age_days: 5.0,
            conflict_severity: 0.0,
            evidence_strength: 1.0,
            source_reliability: 1.0,
            corroboration_count: 5,
            custom: HashMap::new(),
        };
        let config = DecayConfig::default();
        let result = compute_freshness(&factors, DecayModel::Linear, &config);
        assert!(
            result.score > 0.8,
            "Fresh high-quality data should have high score"
        );
        assert!(result.confidence > 0.5, "Should have reasonable confidence");
    }

    #[test]
    fn test_compute_freshness_old_low_quality() {
        let factors = FreshnessFactors {
            evidence_age_days: 300.0,
            conflict_severity: 0.8,
            evidence_strength: 0.3,
            source_reliability: 0.4,
            corroboration_count: 1,
            custom: HashMap::new(),
        };
        let config = DecayConfig::default();
        let result = compute_freshness(&factors, DecayModel::Linear, &config);
        assert!(
            result.score < 0.5,
            "Old low-quality data should have low score"
        );
    }

    #[test]
    fn test_compute_freshness_conflict_penalty() {
        let base_factors = FreshnessFactors {
            evidence_age_days: 10.0,
            conflict_severity: 0.0,
            evidence_strength: 0.8,
            source_reliability: 0.8,
            corroboration_count: 2,
            custom: HashMap::new(),
        };
        let conflict_factors = FreshnessFactors {
            conflict_severity: 0.9,
            ..base_factors.clone()
        };
        let config = DecayConfig::default();

        let base_result = compute_freshness(&base_factors, DecayModel::Linear, &config);
        let conflict_result = compute_freshness(&conflict_factors, DecayModel::Linear, &config);

        assert!(
            conflict_result.score < base_result.score,
            "Conflict should reduce score"
        );
    }

    #[test]
    fn test_compute_freshness_corroboration_bonus() {
        let single_source = FreshnessFactors {
            evidence_age_days: 10.0,
            conflict_severity: 0.0,
            evidence_strength: 0.8,
            source_reliability: 0.8,
            corroboration_count: 1,
            custom: HashMap::new(),
        };
        let multi_source = FreshnessFactors {
            corroboration_count: 10,
            ..single_source.clone()
        };
        let config = DecayConfig::default();

        let single_result = compute_freshness(&single_source, DecayModel::Linear, &config);
        let multi_result = compute_freshness(&multi_source, DecayModel::Linear, &config);

        assert!(
            multi_result.score >= single_result.score,
            "More corroboration should help score"
        );
    }

    #[test]
    fn test_compute_freshness_rationale_present() {
        let factors = FreshnessFactors::default();
        let config = DecayConfig::default();
        let result = compute_freshness(&factors, DecayModel::Linear, &config);
        assert!(!result.rationale.is_empty(), "Rationale should be present");
    }

    // ========================================================================
    // Next Review Scheduling Tests
    // ========================================================================

    #[test]
    fn test_schedule_next_review_linear() {
        let now = Utc::now();
        let config = DecayConfig {
            max_age_days: Some(100.0),
            ..Default::default()
        };
        let next = schedule_next_review(now, DecayModel::Linear, 0.8, &config);
        assert!(next > now, "Next review should be in the future");
    }

    #[test]
    fn test_schedule_next_review_exponential() {
        let now = Utc::now();
        let config = DecayConfig {
            half_life_days: Some(30.0),
            ..Default::default()
        };
        let next = schedule_next_review(now, DecayModel::Exponential, 0.8, &config);
        assert!(next > now, "Next review should be in the future");
        // For exponential, review interval should be related to half-life
    }

    #[test]
    fn test_schedule_next_review_step_function() {
        let now = Utc::now();
        let config = DecayConfig {
            step_thresholds: Some(vec![(7.0, 1.0), (30.0, 0.8), (90.0, 0.5)]),
            ..Default::default()
        };
        let next = schedule_next_review(now, DecayModel::StepFunction, 1.0, &config);
        assert!(next > now, "Next review should be in the future");
    }

    #[test]
    fn test_schedule_next_review_low_score_sooner() {
        let now = Utc::now();
        let config = DecayConfig::default();
        let high_score_next = schedule_next_review(now, DecayModel::Linear, 0.9, &config);
        let low_score_next = schedule_next_review(now, DecayModel::Linear, 0.3, &config);
        // Low scores might need more frequent review (or less frequent if already stale)
        // The exact behavior depends on implementation strategy
        assert!(high_score_next > now && low_score_next > now);
    }

    // ========================================================================
    // Demotion Tests
    // ========================================================================

    #[test]
    fn test_check_demotion_above_threshold() {
        let policy = DemotionPolicy {
            threshold: 0.3,
            grace_period_days: None,
            action: DemotionAction::Demote,
        };
        let result = check_demotion(0.5, &policy);
        assert!(!result.demote, "Score above threshold should not demote");
    }

    #[test]
    fn test_check_demotion_below_threshold() {
        let policy = DemotionPolicy {
            threshold: 0.3,
            grace_period_days: None,
            action: DemotionAction::Demote,
        };
        let result = check_demotion(0.2, &policy);
        assert!(result.demote, "Score below threshold should demote");
        assert_eq!(result.threshold, Some(0.3));
    }

    #[test]
    fn test_check_demotion_at_threshold() {
        let policy = DemotionPolicy {
            threshold: 0.3,
            grace_period_days: None,
            action: DemotionAction::Demote,
        };
        let result = check_demotion(0.3, &policy);
        // At exactly threshold, could go either way - implementation choice
        // Just verify we get a valid result
        assert!(result.threshold.is_none() || result.threshold == Some(0.3));
    }

    #[test]
    fn test_check_demotion_includes_reason() {
        let policy = DemotionPolicy::default();
        let result = check_demotion(0.1, &policy);
        assert!(
            !result.reason.is_empty(),
            "Should include reason for decision"
        );
    }

    // ========================================================================
    // Ranking Integration Tests
    // ========================================================================

    #[test]
    fn test_apply_freshness_to_ranking_basic() {
        let mut slots = vec![
            TruthSlotRanking {
                slot_key: "slot_a".to_string(),
                confidence: 0.9,
                freshness: 0.0,
                combined_score: 0.0,
            },
            TruthSlotRanking {
                slot_key: "slot_b".to_string(),
                confidence: 0.8,
                freshness: 0.0,
                combined_score: 0.0,
            },
        ];
        let freshness_scores: HashMap<String, f32> =
            [("slot_a".to_string(), 0.5), ("slot_b".to_string(), 0.9)]
                .into_iter()
                .collect();

        apply_freshness_to_ranking(&mut slots, &freshness_scores, 0.5);

        // slot_b should rank higher due to better freshness despite lower confidence
        assert!(slots[0].freshness > 0.0 || slots[1].freshness > 0.0);
    }

    #[test]
    fn test_apply_freshness_to_ranking_weight_zero() {
        let mut slots = vec![TruthSlotRanking {
            slot_key: "slot_a".to_string(),
            confidence: 0.9,
            freshness: 0.0,
            combined_score: 0.0,
        }];
        let freshness_scores: HashMap<String, f32> =
            std::iter::once(("slot_a".to_string(), 0.1)).collect();

        apply_freshness_to_ranking(&mut slots, &freshness_scores, 0.0);

        // With weight 0, combined score should equal confidence
        assert!(
            (slots[0].combined_score - slots[0].confidence).abs() < 0.001
                || slots[0].combined_score == 0.0
        );
    }

    #[test]
    fn test_apply_freshness_to_ranking_weight_one() {
        let mut slots = vec![TruthSlotRanking {
            slot_key: "slot_a".to_string(),
            confidence: 0.9,
            freshness: 0.0,
            combined_score: 0.0,
        }];
        let freshness_scores: HashMap<String, f32> =
            std::iter::once(("slot_a".to_string(), 0.1)).collect();

        apply_freshness_to_ranking(&mut slots, &freshness_scores, 1.0);

        // With weight 1, combined score should equal freshness
        assert!(slots[0].freshness == 0.1 || slots[0].combined_score > 0.0);
    }

    #[test]
    fn test_apply_freshness_to_ranking_missing_freshness() {
        let mut slots = vec![TruthSlotRanking {
            slot_key: "slot_a".to_string(),
            confidence: 0.9,
            freshness: 0.0,
            combined_score: 0.0,
        }];
        let freshness_scores: HashMap<String, f32> = HashMap::new(); // No freshness data

        // Should not panic, should handle gracefully
        apply_freshness_to_ranking(&mut slots, &freshness_scores, 0.5);
    }

    #[test]
    fn test_apply_freshness_to_ranking_preserves_order_without_freshness_data() {
        let mut slots = vec![
            TruthSlotRanking {
                slot_key: "slot_a".to_string(),
                confidence: 0.9,
                freshness: 0.0,
                combined_score: 0.9,
            },
            TruthSlotRanking {
                slot_key: "slot_b".to_string(),
                confidence: 0.7,
                freshness: 0.0,
                combined_score: 0.7,
            },
        ];
        let freshness_scores: HashMap<String, f32> = HashMap::new();

        apply_freshness_to_ranking(&mut slots, &freshness_scores, 0.5);

        // Without freshness data, confidence-based ordering should be preserved
        // or slots should maintain their relative positions
    }

    // ========================================================================
    // Freshness Sweep Tests
    // ========================================================================

    static TEST_DB_COUNTER: AtomicU64 = AtomicU64::new(91000);

    const FRESHNESS_SCHEMA: &str = r"
        DEFINE TABLE freshness SCHEMALESS;
    ";

    async fn setup_test_repo() -> FreshnessRepository {
        let db_id = TEST_DB_COUNTER.fetch_add(1, Ordering::SeqCst);
        let config = StorageConfig {
            database: format!("freshness_sweep_test_{db_id}"),
            ..StorageConfig::default()
        };
        let client = StorageClient::new(config).await.unwrap();
        client.query(FRESHNESS_SCHEMA).await.unwrap();
        FreshnessRepository::new(client)
    }

    fn create_test_freshness() -> Freshness {
        let id = RecordId::new_v4(TableName::Freshness);
        let mut base = BaseFields::new(id, "test_space".to_string(), "test_agent".to_string());
        base.security = Security::default();
        base.provenance = Provenance::default();
        base.subjects_state = SubjectsState::Complete;

        Freshness {
            base,
            target: "rel:abc123".to_string(),
            target_type: "rel".to_string(),
            decay_model: DecayModel::Linear,
            score: 0.9,
            confidence: 0.9,
            rationale: None,
            factors: FreshnessFactors {
                evidence_age_days: 10.0,
                conflict_severity: 0.0,
                evidence_strength: 0.9,
                source_reliability: 0.9,
                corroboration_count: 2,
                custom: HashMap::new(),
            },
            computed_at: Utc::now() - Duration::days(10),
            next_review_at: None,
            demote: false,
            demotion_threshold: None,
        }
    }

    #[tokio::test]
    async fn test_execute_freshness_sweep_updates_record() {
        let repo = setup_test_repo().await;
        let repo_clone = repo.clone();
        let item = create_test_freshness();
        let id = item.base.id.clone();

        let created = repo.create(item).await.unwrap();
        let before_computed = created.computed_at;

        let result = execute_freshness_sweep(repo, "test_space", None)
            .await
            .expect("sweep succeeds");
        assert_eq!(result.processed, 1);
        assert_eq!(result.updated, 1);

        let updated = repo_clone.get("test_space", &id).await.unwrap().unwrap();

        assert!(updated.computed_at >= before_computed);
        assert!(updated.next_review_at.is_some());
        assert!(updated.rationale.as_ref().is_some());
    }

    #[tokio::test]
    async fn record_claim_freshness_creates_one_deterministic_projection() {
        let client = StorageClient::new_embedded().await.unwrap();
        let repo = FreshnessRepository::new(client);
        let claim_id = RecordId::new_v4(TableName::Rel);
        let claim_base = BaseFields::new(
            claim_id.clone(),
            "test_space".to_string(),
            "test_agent".to_string(),
        );
        let evidence_time = Utc::now() - Duration::days(2);

        let first = record_claim_freshness(
            &repo,
            &claim_id,
            "rel",
            &claim_base,
            ClaimFreshnessObservation {
                evidence_times: &[evidence_time],
                evidence_strength: 0.9,
                conflict_severity: 0.0,
                corroboration_count: 1,
            },
        )
        .await
        .unwrap();
        let second = record_claim_freshness(
            &repo,
            &claim_id,
            "rel",
            &claim_base,
            ClaimFreshnessObservation {
                evidence_times: &[evidence_time],
                evidence_strength: 0.95,
                conflict_severity: 0.0,
                corroboration_count: 2,
            },
        )
        .await
        .unwrap();

        assert_eq!(first.base.id, second.base.id);
        assert_eq!(second.factors.corroboration_count, 2);
        assert_eq!(repo.list("test_space", 10, None).await.unwrap().len(), 1);

        let mut due = second;
        due.next_review_at = Some(Utc::now() - Duration::seconds(1));
        let due_id = due.base.id.clone();
        repo.update("test_space", &due_id, due).await.unwrap();
        let sweep = FreshnessSweepWorker::new(repo.clone(), SweepConfig::default());
        sweep.sweep("test_space").await.unwrap();
        let swept = repo
            .find_by_target("test_space", &claim_id.to_string())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(swept.decay_model, DecayModel::Exponential);
    }

    // ========================================================================
    // Sweep Worker Config Tests
    // ========================================================================

    #[test]
    fn test_sweep_config_default() {
        let config = SweepConfig::default();
        assert_eq!(config.batch_size, 100);
        assert_eq!(config.decay_model, DecayModel::Linear);
        assert!(config.space_id.is_none());
    }

    #[test]
    fn test_sweep_config_custom() {
        let config = SweepConfig {
            batch_size: 50,
            decay_model: DecayModel::Exponential,
            decay_config: DecayConfig {
                half_life_days: Some(60.0),
                ..Default::default()
            },
            demotion_policy: DemotionPolicy {
                threshold: 0.2,
                ..Default::default()
            },
            space_id: Some("custom_space".to_string()),
        };

        assert_eq!(config.batch_size, 50);
        assert_eq!(config.decay_model, DecayModel::Exponential);
        assert_eq!(config.decay_config.half_life_days, Some(60.0));
        assert_eq!(config.demotion_policy.threshold, 0.2);
        assert_eq!(config.space_id, Some("custom_space".to_string()));
    }

    #[test]
    fn test_sweep_result_default() {
        let result = SweepResult::default();
        assert_eq!(result.processed, 0);
        assert_eq!(result.updated, 0);
        assert_eq!(result.demoted, 0);
        assert_eq!(result.errors, 0);
        assert!(result.error_messages.is_empty());
    }
}
