//! Identity candidate management.
//!
//! Identity candidates represent potential entity merges that need review.
//! They capture signals (evidence) for why two entities might be the same.

use elephant_types::RecordId;
use serde::{Deserialize, Serialize};

/// Status of an identity candidate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum CandidateStatus {
    /// Awaiting review.
    #[default]
    Pending,
    /// Approved and merged.
    Approved,
    /// Rejected - entities are distinct.
    Rejected,
}

/// Signals that indicate two entities might be the same.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct IdentitySignals {
    /// Matching identity keys (e.g., same email, same phone).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub matching_keys: Vec<String>,
    /// Matching handles (e.g., same username).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub matching_handles: Vec<String>,
    /// Name similarity score (0.0 to 1.0).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name_similarity: Option<f64>,
    /// Co-occurrence evidence (shared events/contexts).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub co_occurrences: Vec<RecordId>,
    /// Additional custom signals.
    #[serde(default, skip_serializing_if = "std::collections::HashMap::is_empty")]
    pub custom: std::collections::HashMap<String, serde_json::Value>,
}

impl IdentitySignals {
    /// Creates empty signals.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a matching identity key.
    pub fn add_matching_key(&mut self, key: impl Into<String>) {
        self.matching_keys.push(key.into());
    }

    /// Adds a matching handle.
    pub fn add_matching_handle(&mut self, handle: impl Into<String>) {
        self.matching_handles.push(handle.into());
    }

    /// Sets the name similarity score.
    #[must_use]
    pub const fn with_name_similarity(mut self, score: f64) -> Self {
        self.name_similarity = Some(if score < 0.0 {
            0.0
        } else if score > 1.0 {
            1.0
        } else {
            score
        });
        self
    }

    /// Adds a co-occurrence event.
    pub fn add_co_occurrence(&mut self, record: RecordId) {
        self.co_occurrences.push(record);
    }

    /// Returns true if there are any signals.
    #[must_use]
    pub fn has_signals(&self) -> bool {
        !self.matching_keys.is_empty()
            || !self.matching_handles.is_empty()
            || self.name_similarity.is_some()
            || !self.co_occurrences.is_empty()
            || !self.custom.is_empty()
    }

    /// Computes a heuristic confidence score based on signals.
    ///
    /// The scoring algorithm:
    /// - Identity key matches start at 0.9 base, with +0.05 per additional key (up to 1.0)
    /// - Handle matches contribute 0.7 each
    /// - Name similarity contributes 0.3 * `similarity_score`
    /// - Co-occurrences contribute up to 0.2 total
    ///
    /// Multiple signals of the same type provide diminishing returns via averaging,
    /// but multiple identity keys boost confidence toward 1.0 for auto-merge eligibility.
    #[must_use]
    pub fn compute_confidence(&self) -> f64 {
        let mut score = 0.0;
        let mut weight = 0.0;

        // Identity key matches are strong signals
        // Multiple keys boost confidence higher (0.9 -> 0.95 -> 1.0)
        if !self.matching_keys.is_empty() {
            let base = 0.9;
            #[allow(clippy::cast_precision_loss)]
            let boost = 0.05 * (self.matching_keys.len() - 1).min(2) as f64;
            #[allow(clippy::cast_precision_loss)]
            let len_f64 = self.matching_keys.len() as f64;
            score += (base + boost) * len_f64;
            weight += len_f64;
        }

        // Handle matches are medium signals
        if !self.matching_handles.is_empty() {
            #[allow(clippy::cast_precision_loss)]
            let len_f64 = self.matching_handles.len() as f64;
            score += 0.7 * len_f64;
            weight += len_f64;
        }

        // Name similarity is a weak signal
        if let Some(name_sim) = self.name_similarity {
            score += 0.3 * name_sim;
            weight += 1.0;
        }

        // Co-occurrences are weak signals
        if !self.co_occurrences.is_empty() {
            #[allow(clippy::cast_precision_loss)]
            let co_weight = (self.co_occurrences.len() as f64).min(5.0) / 5.0;
            score += 0.2 * co_weight;
            weight += 1.0;
        }

        if weight > f64::EPSILON {
            (score / weight).clamp(0.0, 1.0)
        } else {
            0.0
        }
    }
}

/// An identity candidate representing a potential entity merge.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IdentityCandidate {
    /// First entity.
    pub entity_a: RecordId,
    /// Second entity.
    pub entity_b: RecordId,
    /// Signals supporting the merge.
    pub signals: IdentitySignals,
    /// Computed confidence score.
    pub confidence: f64,
    /// Review status.
    pub status: CandidateStatus,
    /// Actor who proposed this candidate.
    pub proposed_by: String,
    /// Reason for proposal.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

impl IdentityCandidate {
    /// Creates a new identity candidate.
    #[must_use]
    pub fn new(entity_a: RecordId, entity_b: RecordId, proposed_by: impl Into<String>) -> Self {
        let signals = IdentitySignals::new();
        let confidence = signals.compute_confidence();
        Self {
            entity_a,
            entity_b,
            signals,
            confidence,
            status: CandidateStatus::Pending,
            proposed_by: proposed_by.into(),
            reason: None,
        }
    }

    /// Creates a candidate with signals.
    #[must_use]
    pub fn with_signals(mut self, signals: IdentitySignals) -> Self {
        self.confidence = signals.compute_confidence();
        self.signals = signals;
        self
    }

    /// Sets the confidence score explicitly.
    #[must_use]
    pub const fn with_confidence(mut self, confidence: f64) -> Self {
        self.confidence = confidence.clamp(0.0, 1.0);
        self
    }

    /// Sets the reason.
    #[must_use]
    pub fn with_reason(mut self, reason: impl Into<String>) -> Self {
        self.reason = Some(reason.into());
        self
    }

    /// Approves the candidate.
    pub const fn approve(&mut self) {
        self.status = CandidateStatus::Approved;
    }

    /// Rejects the candidate.
    pub const fn reject(&mut self) {
        self.status = CandidateStatus::Rejected;
    }

    /// Returns true if the candidate is pending.
    #[must_use]
    pub fn is_pending(&self) -> bool {
        self.status == CandidateStatus::Pending
    }

    /// Returns true if the candidate should auto-merge (high confidence, deterministic key).
    #[must_use]
    pub fn should_auto_merge(&self) -> bool {
        // Auto-merge if confidence is very high AND has identity key matches
        self.confidence >= 0.95 && !self.signals.matching_keys.is_empty()
    }
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::unwrap_used,
        clippy::mixed_attributes_style,
        clippy::similar_names,
        clippy::type_complexity,
        clippy::float_cmp,
        clippy::const_is_empty,
        clippy::expect_used,
        clippy::panic,
        unused_imports,
        dead_code,
        clippy::too_many_lines,
        clippy::cognitive_complexity
    )]
    use super::*;
    use elephant_types::ids::TableName;

    #[test]
    fn test_identity_signals_empty() {
        let signals = IdentitySignals::new();
        assert!(!signals.has_signals());
        assert_eq!(signals.compute_confidence(), 0.0);
    }

    #[test]
    fn test_identity_signals_with_keys() {
        let mut signals = IdentitySignals::new();
        signals.add_matching_key("email:test@example.com");

        assert!(signals.has_signals());
        let confidence = signals.compute_confidence();
        assert!(
            confidence > 0.8,
            "Identity key match should give high confidence"
        );
    }

    #[test]
    fn test_identity_signals_with_handles() {
        let mut signals = IdentitySignals::new();
        signals.add_matching_handle("@testuser");

        assert!(signals.has_signals());
        let confidence = signals.compute_confidence();
        assert!(
            confidence > 0.5,
            "Handle match should give medium confidence"
        );
    }

    #[test]
    fn test_identity_signals_with_name_similarity() {
        let signals = IdentitySignals::new().with_name_similarity(0.9);

        assert!(signals.has_signals());
        let confidence = signals.compute_confidence();
        assert!(
            confidence > 0.2,
            "Name similarity should give some confidence"
        );
    }

    #[test]
    fn test_identity_candidate_creation() {
        let entity_a = RecordId::new_v4(TableName::Entity);
        let entity_b = RecordId::new_v4(TableName::Entity);

        let candidate = IdentityCandidate::new(entity_a, entity_b, "test_system");

        assert!(candidate.is_pending());
        assert!(!candidate.should_auto_merge());
    }

    #[test]
    fn test_identity_candidate_with_signals() {
        let entity_a = RecordId::new_v4(TableName::Entity);
        let entity_b = RecordId::new_v4(TableName::Entity);

        let mut signals = IdentitySignals::new();
        signals.add_matching_key("email:same@example.com");
        signals.add_matching_key("phone:+1234567890");

        let candidate =
            IdentityCandidate::new(entity_a, entity_b, "test_system").with_signals(signals);

        assert!(
            candidate.confidence > 0.9,
            "Multiple identity keys should give very high confidence"
        );
        assert!(
            candidate.should_auto_merge(),
            "High confidence with identity keys should auto-merge"
        );
    }

    #[test]
    fn test_identity_candidate_status_changes() {
        let entity_a = RecordId::new_v4(TableName::Entity);
        let entity_b = RecordId::new_v4(TableName::Entity);

        let mut candidate = IdentityCandidate::new(entity_a, entity_b, "test_system");

        assert!(candidate.is_pending());

        candidate.approve();
        assert_eq!(candidate.status, CandidateStatus::Approved);
        assert!(!candidate.is_pending());
    }

    #[test]
    fn test_identity_candidate_reject() {
        let entity_a = RecordId::new_v4(TableName::Entity);
        let entity_b = RecordId::new_v4(TableName::Entity);

        let mut candidate = IdentityCandidate::new(entity_a, entity_b, "test_system");
        assert!(candidate.is_pending());

        candidate.reject();
        assert_eq!(candidate.status, CandidateStatus::Rejected);
        assert!(!candidate.is_pending());
    }

    #[test]
    fn test_confidence_three_matching_keys_reaches_max() {
        let mut signals = IdentitySignals::new();
        signals.add_matching_key("email:test@example.com");
        signals.add_matching_key("phone:+1234567890");
        signals.add_matching_key("ssn:123-45-6789");

        let confidence = signals.compute_confidence();
        // With 3 keys: base=0.9, boost=0.1, score = 1.0 * 3 = 3.0, weight = 3
        // Result = 3.0/3 = 1.0, clamped to 1.0
        assert!(
            confidence >= 0.99,
            "Three identity keys should give maximum confidence: {confidence}"
        );
    }

    #[test]
    fn test_confidence_handles_averaging() {
        let mut signals = IdentitySignals::new();
        signals.add_matching_handle("@user1");
        signals.add_matching_handle("@user2");

        let confidence = signals.compute_confidence();
        // With 2 handles: score = 0.7 * 2 = 1.4, weight = 2
        // Result = 1.4/2 = 0.7
        assert!(
            (confidence - 0.7).abs() < 0.01,
            "Two handles should give 0.7 confidence: {confidence}"
        );
    }

    #[test]
    fn test_confidence_combined_signals() {
        let mut signals = IdentitySignals::new();
        signals.add_matching_key("email:test@example.com"); // 0.9 weight 1
        signals.add_matching_handle("@user"); // 0.7 weight 1
        let signals = signals.with_name_similarity(0.8); // 0.3 * 0.8 = 0.24 weight 1

        let confidence = signals.compute_confidence();
        // Total: 0.9 + 0.7 + 0.24 = 1.84, weight = 3
        // Result = 1.84/3 ≈ 0.613
        assert!(
            confidence > 0.5 && confidence < 0.75,
            "Combined signals should give moderate-high confidence: {confidence}"
        );
    }

    #[test]
    fn test_confidence_name_similarity_boundaries() {
        let signals_zero = IdentitySignals::new().with_name_similarity(0.0);
        let signals_one = IdentitySignals::new().with_name_similarity(1.0);

        let conf_zero = signals_zero.compute_confidence();
        let conf_one = signals_one.compute_confidence();

        assert!(
            (0.0..0.1).contains(&conf_zero),
            "Name similarity 0.0 should give near-zero confidence: {conf_zero}"
        );
        assert!(
            (0.29..=0.31).contains(&conf_one),
            "Name similarity 1.0 should give 0.3 confidence: {conf_one}"
        );
    }

    #[test]
    fn test_confidence_name_similarity_clamping() {
        // Test that out-of-range values are clamped
        let signals_low = IdentitySignals::new().with_name_similarity(-0.5);
        let signals_high = IdentitySignals::new().with_name_similarity(1.5);

        assert!(signals_low.name_similarity.unwrap() >= 0.0);
        assert!(signals_high.name_similarity.unwrap() <= 1.0);
    }

    #[test]
    fn test_confidence_co_occurrence_scaling() {
        let mut signals_one = IdentitySignals::new();
        signals_one.add_co_occurrence(RecordId::new_v4(TableName::Rel));

        let mut signals_five = IdentitySignals::new();
        for _ in 0..5 {
            signals_five.add_co_occurrence(RecordId::new_v4(TableName::Rel));
        }

        let conf_one = signals_one.compute_confidence();
        let conf_five = signals_five.compute_confidence();

        // 1 co-occurrence: 0.2 * (1/5) = 0.04, weight 1 -> 0.04
        // 5 co-occurrences: 0.2 * (5/5) = 0.2, weight 1 -> 0.2
        assert!(
            conf_one < conf_five,
            "More co-occurrences should give higher confidence"
        );
        assert!(
            conf_five <= 0.21,
            "Max co-occurrence confidence should be ~0.2: {conf_five}"
        );
    }

    #[test]
    fn test_should_auto_merge_at_boundary() {
        let entity_a = RecordId::new_v4(TableName::Entity);
        let entity_b = RecordId::new_v4(TableName::Entity);

        let mut signals = IdentitySignals::new();
        signals.add_matching_key("email:test@example.com");
        signals.add_matching_key("phone:+1234567890");

        let candidate =
            IdentityCandidate::new(entity_a, entity_b, "test_system").with_signals(signals);

        // Confidence should be exactly 0.95
        assert!(
            candidate.confidence >= 0.95,
            "With 2 keys, confidence should be >= 0.95: {}",
            candidate.confidence
        );
        assert!(candidate.should_auto_merge());
    }

    #[test]
    fn test_should_auto_merge_high_confidence_no_keys() {
        let entity_a = RecordId::new_v4(TableName::Entity);
        let entity_b = RecordId::new_v4(TableName::Entity);

        // Create a candidate with high confidence but no identity keys
        let candidate =
            IdentityCandidate::new(entity_a, entity_b, "test_system").with_confidence(0.99);

        // Should NOT auto-merge without identity keys
        assert!(
            !candidate.should_auto_merge(),
            "High confidence without identity keys should not auto-merge"
        );
    }

    #[test]
    fn test_custom_signals_has_signals() {
        let mut signals = IdentitySignals::new();
        signals
            .custom
            .insert("biometric_match".to_string(), serde_json::json!(0.95));

        assert!(
            signals.has_signals(),
            "Custom signals should count as having signals"
        );
    }
}
