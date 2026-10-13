//! ABAC evaluation engine.

use std::collections::HashSet;

use elephant_types::records::base::{Security, SubjectsState};
use elephant_types::records::Entity;
use elephant_types::{Link, RecordId};
use serde::{Deserialize, Serialize};

/// A clearance held by a principal.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Clearance {
    /// Policy ID this clearance applies to.
    pub policy_id: String,
    /// Maximum level allowed.
    pub max_level: i32,
    /// Labels the principal has clearance for.
    pub labels: HashSet<String>,
    /// Handling caveats the principal can access.
    pub handling_allow: HashSet<String>,
}

/// Principal claims from the authentication token.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Principal {
    /// Space ID the principal is authorized for.
    pub space_id: String,
    /// Operation scopes granted.
    pub scopes: HashSet<String>,
    /// Clearances for different policies.
    pub clearances: Vec<Clearance>,
    /// Allowlist of subject entities (if restricted).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subject_allowlist: Option<HashSet<RecordId>>,
    /// Required purposes.
    #[serde(default)]
    pub purpose: HashSet<String>,
}

/// Record attributes for access evaluation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecordAttributes {
    /// The record's `space_id`.
    pub space_id: String,
    /// Security labels on the record.
    pub security: Security,
    /// Subjects associated with the record.
    #[serde(default)]
    pub subjects: Vec<Link<Entity>>,
    /// State of subject population.
    #[serde(default)]
    pub subjects_state: SubjectsState,
}

/// Access decision result.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AccessDecision {
    /// Access granted.
    Allow,
    /// Access denied with reason.
    Deny(String),
}

impl AccessDecision {
    /// Returns true if access is allowed.
    #[must_use]
    pub const fn is_allowed(&self) -> bool {
        matches!(self, Self::Allow)
    }
}

/// ABAC evaluation engine.
///
/// The engine is configured with required purposes and evaluates access
/// decisions based on principal claims and record security attributes.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct AbacEngine {
    /// Required purpose labels (if any).
    #[serde(default)]
    required_purposes: HashSet<String>,
}

impl AbacEngine {
    /// Creates a new ABAC engine.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets required purposes for access.
    #[must_use]
    pub fn with_required_purposes(mut self, purposes: HashSet<String>) -> Self {
        self.required_purposes = purposes;
        self
    }

    /// Evaluates access for a principal against a record.
    ///
    /// Access is granted if ALL of the following hold:
    /// 1. `space_id` matches
    /// 2. required operation scope is present
    /// 3. a clearance exists for the record's `policy_id`
    /// 4. level <= `max_level`
    /// 5. labels ⊆ clearance.labels
    /// 6. handling ⊆ `clearance.handling_allow`
    /// 7. if `subject_allowlist` exists, subjects must be non-empty and ⊆ allowlist
    /// 8. if purpose is required, token purpose includes it
    #[must_use = "access decisions must not be ignored"]
    pub fn evaluate(
        &self,
        principal: &Principal,
        record: &RecordAttributes,
        required_scope: &str,
    ) -> AccessDecision {
        // Rule 1: space_id must match
        if principal.space_id != record.space_id {
            return AccessDecision::Deny("space_id mismatch".to_string());
        }

        // Rule 2: required scope must be present
        if !principal.scopes.contains(required_scope) {
            return AccessDecision::Deny(format!("missing scope: {required_scope}"));
        }

        // Rule 3: find clearance for this policy_id
        let clearance = principal
            .clearances
            .iter()
            .find(|c| c.policy_id == record.security.policy_id);

        let Some(clearance) = clearance else {
            let policy_id = &record.security.policy_id;
            return AccessDecision::Deny(format!("no clearance for policy: {policy_id}"));
        };

        // Rule 4: level <= max_level
        if record.security.level > clearance.max_level {
            return AccessDecision::Deny(format!(
                "level {} exceeds max {}",
                record.security.level, clearance.max_level
            ));
        }

        // Rule 5: labels ⊆ clearance.labels
        for label in &record.security.labels {
            if !clearance.labels.contains(label) {
                return AccessDecision::Deny(format!("missing label clearance: {label}"));
            }
        }

        // Rule 6: handling ⊆ clearance.handling_allow
        for handling in &record.security.handling {
            if !clearance.handling_allow.contains(handling) {
                return AccessDecision::Deny(format!("handling not allowed: {handling}"));
            }
        }

        // Rule 7: subject allowlist check
        if let Some(ref allowlist) = principal.subject_allowlist {
            // Check if subjects_state is complete
            if record.subjects_state != SubjectsState::Complete {
                // Check if subjectless_ok handling is present
                if !record
                    .security
                    .handling
                    .iter()
                    .any(|h| h == "handling:subjectless_ok")
                {
                    return AccessDecision::Deny(
                        "subjects_state not complete and no subjectless_ok handling".to_string(),
                    );
                }
            }

            // Subjects must be non-empty
            if record.subjects.is_empty() {
                if !record
                    .security
                    .handling
                    .iter()
                    .any(|h| h == "handling:subjectless_ok")
                {
                    return AccessDecision::Deny(
                        "empty subjects with subject_allowlist".to_string(),
                    );
                }
            } else {
                // All subjects must be in allowlist
                for subject in &record.subjects {
                    if !allowlist.contains(subject.id()) {
                        return AccessDecision::Deny(format!(
                            "subject {} not in allowlist",
                            subject.id()
                        ));
                    }
                }
            }
        }

        // Rule 8: purpose check
        if !self.required_purposes.is_empty() {
            for purpose in &self.required_purposes {
                if !principal.purpose.contains(purpose) {
                    return AccessDecision::Deny(format!("missing purpose: {purpose}"));
                }
            }
        }

        AccessDecision::Allow
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

    fn make_principal() -> Principal {
        let mut clearance_labels = HashSet::new();
        clearance_labels.insert("comp:alerts".to_string());
        clearance_labels.insert("vis:shared_context".to_string());

        let mut handling = HashSet::new();
        handling.insert("handling:trusted_context".to_string());

        Principal {
            space_id: "tenant_123".to_string(),
            scopes: std::iter::once("read:entity".to_string()).collect(),
            clearances: vec![Clearance {
                policy_id: "tenant_policy_v1".to_string(),
                max_level: 2,
                labels: clearance_labels,
                handling_allow: handling,
            }],
            subject_allowlist: None,
            purpose: HashSet::new(),
        }
    }

    fn make_record() -> RecordAttributes {
        RecordAttributes {
            space_id: "tenant_123".to_string(),
            security: Security {
                policy_id: "tenant_policy_v1".to_string(),
                level: 1,
                labels: vec!["comp:alerts".to_string()],
                handling: vec![],
            },
            subjects: vec![],
            subjects_state: SubjectsState::Unknown,
        }
    }

    #[test]
    fn test_allow_access() {
        let engine = AbacEngine::new();
        let principal = make_principal();
        let record = make_record();

        let decision = engine.evaluate(&principal, &record, "read:entity");
        assert!(decision.is_allowed());
    }

    #[test]
    fn test_deny_space_mismatch() {
        let engine = AbacEngine::new();
        let principal = make_principal();
        let mut record = make_record();
        record.space_id = "other_space".to_string();

        let decision = engine.evaluate(&principal, &record, "read:entity");
        assert!(!decision.is_allowed());
    }

    #[test]
    fn test_deny_missing_scope() {
        let engine = AbacEngine::new();
        let principal = make_principal();
        let record = make_record();

        let decision = engine.evaluate(&principal, &record, "write:entity");
        assert!(!decision.is_allowed());
    }

    #[test]
    fn test_deny_level_exceeded() {
        let engine = AbacEngine::new();
        let principal = make_principal();
        let mut record = make_record();
        record.security.level = 5;

        let decision = engine.evaluate(&principal, &record, "read:entity");
        assert!(!decision.is_allowed());
    }

    #[test]
    fn test_deny_missing_label() {
        let engine = AbacEngine::new();
        let principal = make_principal();
        let mut record = make_record();
        record.security.labels.push("comp:confidant".to_string());

        let decision = engine.evaluate(&principal, &record, "read:entity");
        assert!(!decision.is_allowed());
    }

    #[test]
    fn test_subject_allowlist_enforcement() {
        let engine = AbacEngine::new();
        let mut principal = make_principal();

        let child_id = RecordId::new_v4(TableName::Entity);
        let mut allowlist = HashSet::new();
        allowlist.insert(child_id.clone());
        principal.subject_allowlist = Some(allowlist);

        let mut record = make_record();
        record.subjects = vec![Link::new(child_id)];
        record.subjects_state = SubjectsState::Complete;

        let decision = engine.evaluate(&principal, &record, "read:entity");
        assert!(decision.is_allowed());
    }

    #[test]
    fn test_subject_not_in_allowlist() {
        let engine = AbacEngine::new();
        let mut principal = make_principal();

        let child_id = RecordId::new_v4(TableName::Entity);
        let other_id = RecordId::new_v4(TableName::Entity);
        let mut allowlist = HashSet::new();
        allowlist.insert(child_id);
        principal.subject_allowlist = Some(allowlist);

        let mut record = make_record();
        record.subjects = vec![Link::new(other_id)];
        record.subjects_state = SubjectsState::Complete;

        let decision = engine.evaluate(&principal, &record, "read:entity");
        assert!(!decision.is_allowed());
    }

    // Rule 3: Missing clearance for policy_id
    #[test]
    fn test_deny_missing_policy_clearance() {
        let engine = AbacEngine::new();
        let principal = make_principal();
        let mut record = make_record();
        record.security.policy_id = "unknown_policy".to_string();

        let decision = engine.evaluate(&principal, &record, "read:entity");
        assert!(!decision.is_allowed());
        assert!(
            matches!(decision, AccessDecision::Deny(msg) if msg.contains("no clearance for policy"))
        );
    }

    // Rule 6: Handling caveat not allowed
    #[test]
    fn test_deny_handling_not_allowed() {
        let engine = AbacEngine::new();
        let principal = make_principal();
        let mut record = make_record();
        record.security.handling = vec!["handling:noforn".to_string()];

        let decision = engine.evaluate(&principal, &record, "read:entity");
        assert!(!decision.is_allowed());
        assert!(
            matches!(decision, AccessDecision::Deny(msg) if msg.contains("handling not allowed"))
        );
    }

    // Rule 8: Purpose check
    #[test]
    fn test_deny_missing_purpose() {
        let mut purposes = HashSet::new();
        purposes.insert("analytics".to_string());
        let engine = AbacEngine::new().with_required_purposes(purposes);

        let principal = make_principal();
        let record = make_record();

        let decision = engine.evaluate(&principal, &record, "read:entity");
        assert!(!decision.is_allowed());
        assert!(matches!(decision, AccessDecision::Deny(msg) if msg.contains("missing purpose")));
    }

    #[test]
    fn test_allow_with_purpose() {
        let mut purposes = HashSet::new();
        purposes.insert("analytics".to_string());
        let engine = AbacEngine::new().with_required_purposes(purposes);

        let mut principal = make_principal();
        principal.purpose.insert("analytics".to_string());
        let record = make_record();

        let decision = engine.evaluate(&principal, &record, "read:entity");
        assert!(decision.is_allowed());
    }

    // Edge case: subjects_state not complete with allowlist
    #[test]
    fn test_deny_subjects_state_not_complete() {
        let engine = AbacEngine::new();
        let mut principal = make_principal();

        let child_id = RecordId::new_v4(TableName::Entity);
        let mut allowlist = HashSet::new();
        allowlist.insert(child_id.clone());
        principal.subject_allowlist = Some(allowlist);

        let mut record = make_record();
        record.subjects = vec![Link::new(child_id)];
        record.subjects_state = SubjectsState::Partial; // Not complete!

        let decision = engine.evaluate(&principal, &record, "read:entity");
        assert!(!decision.is_allowed());
        assert!(
            matches!(decision, AccessDecision::Deny(msg) if msg.contains("subjects_state not complete"))
        );
    }

    // Edge case: subjectless_ok handling allows empty subjects
    #[test]
    fn test_allow_subjectless_ok_with_empty_subjects() {
        let engine = AbacEngine::new();
        let mut principal = make_principal();

        let child_id = RecordId::new_v4(TableName::Entity);
        let mut allowlist = HashSet::new();
        allowlist.insert(child_id);
        principal.subject_allowlist = Some(allowlist);

        // Add handling:subjectless_ok to clearance
        principal.clearances[0]
            .handling_allow
            .insert("handling:subjectless_ok".to_string());

        let mut record = make_record();
        record.subjects = vec![]; // Empty subjects
        record.subjects_state = SubjectsState::Unknown;
        record.security.handling = vec!["handling:subjectless_ok".to_string()];

        let decision = engine.evaluate(&principal, &record, "read:entity");
        assert!(decision.is_allowed());
    }

    // Edge case: subjects_state partial with subjectless_ok
    #[test]
    fn test_allow_partial_subjects_with_subjectless_ok() {
        let engine = AbacEngine::new();
        let mut principal = make_principal();

        let child_id = RecordId::new_v4(TableName::Entity);
        let mut allowlist = HashSet::new();
        allowlist.insert(child_id.clone());
        principal.subject_allowlist = Some(allowlist);

        // Add handling:subjectless_ok to clearance
        principal.clearances[0]
            .handling_allow
            .insert("handling:subjectless_ok".to_string());

        let mut record = make_record();
        record.subjects = vec![Link::new(child_id)];
        record.subjects_state = SubjectsState::Partial; // Partial but has subjectless_ok
        record.security.handling = vec!["handling:subjectless_ok".to_string()];

        let decision = engine.evaluate(&principal, &record, "read:entity");
        assert!(decision.is_allowed());
    }

    // Additional edge case tests

    #[test]
    fn test_multiple_subjects_partial_allowlist_match() {
        let engine = AbacEngine::new();
        let mut principal = make_principal();

        let child_a = RecordId::new_v4(TableName::Entity);
        let child_b = RecordId::new_v4(TableName::Entity);
        let mut allowlist = HashSet::new();
        allowlist.insert(child_a.clone()); // Only child_a in allowlist
        principal.subject_allowlist = Some(allowlist);

        let mut record = make_record();
        record.subjects = vec![Link::new(child_a), Link::new(child_b)]; // Both subjects
        record.subjects_state = SubjectsState::Complete;

        let decision = engine.evaluate(&principal, &record, "read:entity");
        assert!(
            !decision.is_allowed(),
            "Should deny when not all subjects are in allowlist"
        );
        assert!(matches!(decision, AccessDecision::Deny(msg) if msg.contains("not in allowlist")));
    }

    #[test]
    fn test_multiple_labels_partial_match() {
        let engine = AbacEngine::new();
        let principal = make_principal();
        let mut record = make_record();

        // Add two labels, one the principal has clearance for and one they don't
        record.security.labels = vec![
            "comp:alerts".to_string(),  // Principal has this
            "comp:unknown".to_string(), // Principal doesn't have this
        ];

        let decision = engine.evaluate(&principal, &record, "read:entity");
        assert!(
            !decision.is_allowed(),
            "Should deny when any label lacks clearance"
        );
        assert!(matches!(decision, AccessDecision::Deny(msg) if msg.contains("missing label")));
    }

    #[test]
    fn test_multiple_handling_partial_match() {
        let engine = AbacEngine::new();
        let principal = make_principal();
        let mut record = make_record();

        record.security.handling = vec![
            "handling:trusted_context".to_string(), // Principal allows this
            "handling:restricted".to_string(),      // Principal doesn't allow this
        ];

        let decision = engine.evaluate(&principal, &record, "read:entity");
        assert!(
            !decision.is_allowed(),
            "Should deny when any handling lacks clearance"
        );
        assert!(
            matches!(decision, AccessDecision::Deny(msg) if msg.contains("handling not allowed"))
        );
    }

    #[test]
    fn test_level_at_exact_boundary() {
        let engine = AbacEngine::new();
        let principal = make_principal(); // max_level = 2
        let mut record = make_record();

        record.security.level = 2; // Exactly at max
        let decision = engine.evaluate(&principal, &record, "read:entity");
        assert!(
            decision.is_allowed(),
            "Should allow when level equals max_level"
        );

        record.security.level = 3; // One above max
        let decision = engine.evaluate(&principal, &record, "read:entity");
        assert!(
            !decision.is_allowed(),
            "Should deny when level exceeds max_level"
        );
    }

    #[test]
    fn test_empty_clearances() {
        let engine = AbacEngine::new();
        let mut principal = make_principal();
        principal.clearances = vec![]; // No clearances at all

        let record = make_record();
        let decision = engine.evaluate(&principal, &record, "read:entity");

        assert!(
            !decision.is_allowed(),
            "Should deny when no clearances exist"
        );
        assert!(
            matches!(decision, AccessDecision::Deny(msg) if msg.contains("no clearance for policy"))
        );
    }

    #[test]
    fn test_multiple_clearances_correct_one_matched() {
        let engine = AbacEngine::new();
        let mut principal = make_principal();

        // Add another clearance for a different policy
        principal.clearances.push(Clearance {
            policy_id: "other_policy".to_string(),
            max_level: 5,
            labels: HashSet::new(),
            handling_allow: HashSet::new(),
        });

        let record = make_record(); // Uses tenant_policy_v1 policy
        let decision = engine.evaluate(&principal, &record, "read:entity");

        assert!(
            decision.is_allowed(),
            "Should match the correct clearance from multiple"
        );
    }

    #[test]
    fn test_empty_record_labels_and_handling() {
        let engine = AbacEngine::new();
        let principal = make_principal();
        let mut record = make_record();

        record.security.labels = vec![];
        record.security.handling = vec![];

        let decision = engine.evaluate(&principal, &record, "read:entity");
        assert!(
            decision.is_allowed(),
            "Should allow when record has no labels/handling"
        );
    }

    #[test]
    fn test_multiple_purposes_required() {
        let mut purposes = HashSet::new();
        purposes.insert("analytics".to_string());
        purposes.insert("reporting".to_string());
        let engine = AbacEngine::new().with_required_purposes(purposes);

        let mut principal = make_principal();
        principal.purpose.insert("analytics".to_string());
        // Missing "reporting"

        let record = make_record();
        let decision = engine.evaluate(&principal, &record, "read:entity");

        assert!(
            !decision.is_allowed(),
            "Should deny when not all purposes are present"
        );
    }

    #[test]
    fn test_multiple_purposes_all_present() {
        let mut purposes = HashSet::new();
        purposes.insert("analytics".to_string());
        purposes.insert("reporting".to_string());
        let engine = AbacEngine::new().with_required_purposes(purposes);

        let mut principal = make_principal();
        principal.purpose.insert("analytics".to_string());
        principal.purpose.insert("reporting".to_string());
        principal.purpose.insert("extra".to_string()); // Extra is fine

        let record = make_record();
        let decision = engine.evaluate(&principal, &record, "read:entity");

        assert!(
            decision.is_allowed(),
            "Should allow when all required purposes are present"
        );
    }
}

#[cfg(test)]
mod proptests {
    use super::*;
    use proptest::prelude::*;

    /// Strategy to generate random security levels
    fn level_strategy() -> impl Strategy<Value = i32> {
        -10..100i32
    }

    /// Strategy to generate random clearance max levels
    fn max_level_strategy() -> impl Strategy<Value = i32> {
        -10..100i32
    }

    /// Strategy to generate random labels
    fn label_strategy() -> impl Strategy<Value = String> {
        prop_oneof![
            Just("comp:alerts".to_string()),
            Just("comp:health".to_string()),
            Just("comp:location".to_string()),
            Just("vis:team".to_string()),
            Just("vis:private".to_string()),
        ]
    }

    proptest! {
        /// Property: If level > max_level, access must be denied (monotonicity)
        #[test]
        fn prop_level_monotonicity(
            record_level in level_strategy(),
            max_level in max_level_strategy(),
        ) {
            let engine = AbacEngine::new();

            let mut clearance_labels = HashSet::new();
            clearance_labels.insert("comp:alerts".to_string());

            let principal = Principal {
                space_id: "test_space".to_string(),
                scopes: std::iter::once("read:entity".to_string()).collect(),
                clearances: vec![Clearance {
                    policy_id: "test_policy".to_string(),
                    max_level,
                    labels: clearance_labels,
                    handling_allow: HashSet::new(),
                }],
                subject_allowlist: None,
                purpose: HashSet::new(),
            };

            let record = RecordAttributes {
                space_id: "test_space".to_string(),
                security: Security {
                    policy_id: "test_policy".to_string(),
                    level: record_level,
                    labels: vec!["comp:alerts".to_string()],
                    handling: vec![],
                },
                subjects: vec![],
                subjects_state: SubjectsState::Unknown,
            };

            let decision = engine.evaluate(&principal, &record, "read:entity");

            // If level > max_level, must deny
            if record_level > max_level {
                prop_assert!(!decision.is_allowed(),
                    "Level {} > max_level {} should deny", record_level, max_level);
            }
            // If level <= max_level with matching everything else, should allow
            else {
                prop_assert!(decision.is_allowed(),
                    "Level {} <= max_level {} should allow", record_level, max_level);
            }
        }

        /// Property: Deny-by-default - empty clearances always deny
        #[test]
        fn prop_deny_by_default_no_clearances(
            level in level_strategy(),
        ) {
            let engine = AbacEngine::new();

            let principal = Principal {
                space_id: "test_space".to_string(),
                scopes: std::iter::once("read:entity".to_string()).collect(),
                clearances: vec![], // No clearances
                subject_allowlist: None,
                purpose: HashSet::new(),
            };

            let record = RecordAttributes {
                space_id: "test_space".to_string(),
                security: Security {
                    policy_id: "test_policy".to_string(),
                    level,
                    labels: vec![],
                    handling: vec![],
                },
                subjects: vec![],
                subjects_state: SubjectsState::Unknown,
            };

            let decision = engine.evaluate(&principal, &record, "read:entity");
            prop_assert!(!decision.is_allowed(), "Empty clearances should always deny");
        }

        /// Property: Space mismatch always denies
        #[test]
        fn prop_space_mismatch_always_denies(
            principal_space in "[a-z]{3,8}",
            record_space in "[a-z]{3,8}",
        ) {
            // Only test when spaces actually differ
            prop_assume!(principal_space != record_space);

            let engine = AbacEngine::new();

            let mut clearance_labels = HashSet::new();
            clearance_labels.insert("comp:alerts".to_string());

            let principal = Principal {
                space_id: principal_space,
                scopes: std::iter::once("read:entity".to_string()).collect(),
                clearances: vec![Clearance {
                    policy_id: "test_policy".to_string(),
                    max_level: 100,
                    labels: clearance_labels,
                    handling_allow: HashSet::new(),
                }],
                subject_allowlist: None,
                purpose: HashSet::new(),
            };

            let record = RecordAttributes {
                space_id: record_space,
                security: Security {
                    policy_id: "test_policy".to_string(),
                    level: 0,
                    labels: vec!["comp:alerts".to_string()],
                    handling: vec![],
                },
                subjects: vec![],
                subjects_state: SubjectsState::Unknown,
            };

            let decision = engine.evaluate(&principal, &record, "read:entity");
            prop_assert!(!decision.is_allowed(), "Space mismatch should always deny");
        }

        /// Property: Labels form a subset requirement
        /// If record has labels L1, L2 and clearance only has L1, must deny
        #[test]
        fn prop_labels_subset_requirement(
            clearance_labels in prop::collection::hash_set(label_strategy(), 0..5),
            record_labels in prop::collection::vec(label_strategy(), 0..5),
        ) {
            let engine = AbacEngine::new();

            let principal = Principal {
                space_id: "test_space".to_string(),
                scopes: std::iter::once("read:entity".to_string()).collect(),
                clearances: vec![Clearance {
                    policy_id: "test_policy".to_string(),
                    max_level: 100,
                    labels: clearance_labels.clone(),
                    handling_allow: HashSet::new(),
                }],
                subject_allowlist: None,
                purpose: HashSet::new(),
            };

            let record = RecordAttributes {
                space_id: "test_space".to_string(),
                security: Security {
                    policy_id: "test_policy".to_string(),
                    level: 0,
                    labels: record_labels.clone(),
                    handling: vec![],
                },
                subjects: vec![],
                subjects_state: SubjectsState::Unknown,
            };

            let decision = engine.evaluate(&principal, &record, "read:entity");

            // Check if all record labels are in clearance labels
            let all_labels_present = record_labels.iter().all(|l| clearance_labels.contains(l));

            if all_labels_present {
                prop_assert!(decision.is_allowed(),
                    "All labels present, should allow");
            } else {
                prop_assert!(!decision.is_allowed(),
                    "Missing labels, should deny");
            }
        }

        /// Property: Scope requirement - missing scope always denies
        #[test]
        fn prop_scope_requirement(
            has_scope in any::<bool>(),
        ) {
            let engine = AbacEngine::new();

            let scopes = if has_scope {
                std::iter::once("read:entity".to_string()).collect()
            } else {
                HashSet::new()
            };

            let mut clearance_labels = HashSet::new();
            clearance_labels.insert("comp:alerts".to_string());

            let principal = Principal {
                space_id: "test_space".to_string(),
                scopes,
                clearances: vec![Clearance {
                    policy_id: "test_policy".to_string(),
                    max_level: 100,
                    labels: clearance_labels,
                    handling_allow: HashSet::new(),
                }],
                subject_allowlist: None,
                purpose: HashSet::new(),
            };

            let record = RecordAttributes {
                space_id: "test_space".to_string(),
                security: Security {
                    policy_id: "test_policy".to_string(),
                    level: 0,
                    labels: vec!["comp:alerts".to_string()],
                    handling: vec![],
                },
                subjects: vec![],
                subjects_state: SubjectsState::Unknown,
            };

            let decision = engine.evaluate(&principal, &record, "read:entity");

            if has_scope {
                prop_assert!(decision.is_allowed(), "With scope should allow");
            } else {
                prop_assert!(!decision.is_allowed(), "Without scope should deny");
            }
        }
    }
}
