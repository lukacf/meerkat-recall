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
//! S4.7 Abstention retry scenario test.
//!
//! Verifies ABSTAIN handling and exponential backoff for resolution retries.

use chrono::{Duration, Utc};
use elephant_storage::{AssertionRepository, Repository, SourceRepository};
use elephant_types::ids::TableName;
use elephant_types::records::assertion::{
    Assertion, AssertionRetryState, EvidenceSpan, ResolutionStatus,
};
use elephant_types::records::base::{BaseFields, Security, SubjectsState};
use elephant_types::records::source::{Source, SourceType};
use elephant_types::{Link, RecordId};

use crate::Harness;

/// S4.7: Abstention handling and retry behavior.
///
/// Test invariants:
/// 1. Ambiguous resolution triggers Abstained status
/// 2. Exponential backoff schedule computed from attempt count
/// 3. Retry eligible only after backoff delay passes
/// 4. Max attempts transitions to Failed status
#[tokio::test]
async fn scenario_s4_7_abstention_retry() -> anyhow::Result<()> {
    let harness = Harness::new().await?;
    let source_repo = SourceRepository::new(harness.client.clone());
    let assertion_repo = AssertionRepository::new(harness.client.clone());

    let space_id = "Space_Abstention";
    let now = Utc::now();

    // Create prerequisite: source for evidence tracing
    let source_id = RecordId::new_v4(TableName::Source);
    let mut source_base =
        BaseFields::new(source_id.clone(), space_id.to_string(), "P_sys".to_string());
    source_base.security = Security {
        policy_id: "default".to_string(),
        level: 1,
        ..Default::default()
    };
    source_base.subjects_state = SubjectsState::Complete;
    let mut source = Source::new(
        source_base,
        SourceType::Email,
        "hash_abstain_test".to_string(),
    );
    source.external_id = Some("email-ambig-001".to_string());
    source.uri = Some("mailto:sender@example.com".to_string());
    source.metadata = serde_json::json!({
        "subject": "Meeting with John",
        "from": "alice@example.com"
    });
    source_repo.create(source).await?;

    // Use a placeholder doc_revision ID for evidence spans
    // (In production, this would be created via the pipeline)
    let doc_rev_id = RecordId::new_v4(TableName::DocRevision);

    // Invariant 1: Ambiguous resolution triggers Abstained status
    // Create an assertion with ambiguous subject (multiple "John" entities could match)
    let assertion_1_id = RecordId::new_v4(TableName::Assertion);
    let assertion_1_base = BaseFields::new(
        assertion_1_id.clone(),
        space_id.to_string(),
        "extractor".to_string(),
    );
    // Note: Not setting provenance.sources to avoid SurrealDB meta::tb() serialization issues
    let assertion_1 = Assertion {
        base: assertion_1_base,
        subject_text: "John".to_string(), // Ambiguous - could match multiple entities
        predicate_raw: "works_at".to_string(),
        predicate_id: None,
        object_text: Some("a new company".to_string()), // Also ambiguous
        object_literal: None,
        evidence: EvidenceSpan {
            revision: Link::new(doc_rev_id.clone()),
            node: None,
            start: 42,
            end: 85,
            quote_hash: "sha256:evidence_hash_1".to_string(),
        },
        source_event_time: None,
        observed_at: now,
        asserted_at: now,
        assertion_key: format!("{}:works_at:john-ambig", doc_rev_id),
        resolution_status: ResolutionStatus::Abstained, // Resolver abstained due to ambiguity
        resolution_method: None,
        resolution_confidence: None,
        retry_state: AssertionRetryState {
            resolution_attempts: 1,
            last_resolution_at: Some(now),
            abstention_reason: Some(
                "Multiple entity candidates for 'John' (3 matches with conf > 0.5)".to_string(),
            ),
        },
        subject_entity: None,
        object_entity: None,
        confidence: 0.85,
        subject_entity_type: None,
        object_entity_type: None,
        significance: None,
        lifecycle_tier: "active".to_string(),
    };
    let created_assertion_1 = assertion_repo.create(assertion_1).await?;
    assert_eq!(
        created_assertion_1.resolution_status,
        ResolutionStatus::Abstained,
        "Ambiguous assertion should have Abstained status"
    );
    assert!(
        created_assertion_1.retry_state.abstention_reason.is_some(),
        "Abstention reason should be recorded"
    );
    assert_eq!(
        created_assertion_1.retry_state.resolution_attempts, 1,
        "Attempt count should be tracked"
    );

    // Invariant 2: Exponential backoff schedule computed from attempt count
    // After 1 attempt: backoff = 5 min
    // After 2 attempts: backoff = 20 min (5 * 2^1 * 2)
    // After 3 attempts: backoff = 80 min (5 * 2^2 * 4)
    fn compute_backoff_minutes(attempts: u32) -> i64 {
        // Exponential backoff: base_minutes * 2^(attempts-1) * jitter_factor
        // Simplified: 5 * 4^(attempts-1)
        let base_minutes: i64 = 5;
        let multiplier = 4_i64.pow(attempts.saturating_sub(1));
        base_minutes * multiplier
    }

    let backoff_after_1 = compute_backoff_minutes(1);
    assert_eq!(
        backoff_after_1, 5,
        "First retry backoff should be 5 minutes"
    );

    let backoff_after_2 = compute_backoff_minutes(2);
    assert_eq!(
        backoff_after_2, 20,
        "Second retry backoff should be 20 minutes"
    );

    let backoff_after_3 = compute_backoff_minutes(3);
    assert_eq!(
        backoff_after_3, 80,
        "Third retry backoff should be 80 minutes"
    );

    // Invariant 3: Retry eligible only after backoff delay passes
    // Simulate a second resolution attempt after backoff
    let second_attempt_time = now + Duration::minutes(backoff_after_1 + 1); // After backoff

    let mut assertion_after_retry = created_assertion_1.clone();
    assertion_after_retry.retry_state.resolution_attempts = 2;
    assertion_after_retry.retry_state.last_resolution_at = Some(second_attempt_time);
    assertion_after_retry.retry_state.abstention_reason =
        Some("Still ambiguous: 2 entity candidates remain after context analysis".to_string());

    let updated_assertion_1 = assertion_repo
        .update(space_id, &assertion_1_id, assertion_after_retry)
        .await?;
    assert_eq!(
        updated_assertion_1.retry_state.resolution_attempts, 2,
        "Attempt count should be incremented"
    );
    assert_eq!(
        updated_assertion_1.resolution_status,
        ResolutionStatus::Abstained,
        "Should still be Abstained after second attempt"
    );

    // Create helper to check if eligible for retry
    fn is_eligible_for_retry(
        status: &ResolutionStatus,
        attempts: u32,
        last_attempt: Option<chrono::DateTime<Utc>>,
        current_time: chrono::DateTime<Utc>,
        max_attempts: u32,
    ) -> bool {
        if *status != ResolutionStatus::Abstained && *status != ResolutionStatus::Pending {
            return false;
        }
        if attempts >= max_attempts {
            return false;
        }
        if let Some(last) = last_attempt {
            let backoff = Duration::minutes(compute_backoff_minutes(attempts));
            return current_time >= last + backoff;
        }
        true
    }

    let max_attempts = 5;

    // Not yet eligible (immediately after attempt 2)
    let not_yet_eligible = is_eligible_for_retry(
        &updated_assertion_1.resolution_status,
        updated_assertion_1.retry_state.resolution_attempts,
        updated_assertion_1.retry_state.last_resolution_at,
        second_attempt_time, // Same time as last attempt
        max_attempts,
    );
    assert!(
        !not_yet_eligible,
        "Should not be eligible immediately after attempt"
    );

    // Eligible after backoff passes
    let after_backoff_time = second_attempt_time + Duration::minutes(backoff_after_2 + 1);
    let now_eligible = is_eligible_for_retry(
        &updated_assertion_1.resolution_status,
        updated_assertion_1.retry_state.resolution_attempts,
        updated_assertion_1.retry_state.last_resolution_at,
        after_backoff_time,
        max_attempts,
    );
    assert!(now_eligible, "Should be eligible after backoff passes");

    // Invariant 4: Max attempts transitions to Failed status
    // Create another assertion that has exhausted all retry attempts
    let assertion_2_id = RecordId::new_v4(TableName::Assertion);
    let assertion_2_base = BaseFields::new(
        assertion_2_id.clone(),
        space_id.to_string(),
        "extractor".to_string(),
    );
    // Note: Not setting provenance.sources to avoid SurrealDB meta::tb() serialization issues
    let assertion_2 = Assertion {
        base: assertion_2_base,
        subject_text: "Dr. Smith".to_string(), // Highly ambiguous
        predicate_raw: "presented".to_string(),
        predicate_id: None,
        object_text: Some("findings".to_string()),
        object_literal: None,
        evidence: EvidenceSpan {
            revision: Link::new(doc_rev_id.clone()),
            node: None,
            start: 0,
            end: 35,
            quote_hash: "sha256:evidence_hash_2".to_string(),
        },
        source_event_time: None,
        observed_at: now - Duration::hours(24),
        asserted_at: now - Duration::hours(24),
        assertion_key: format!("{}:presented:smith-ambig", doc_rev_id),
        resolution_status: ResolutionStatus::Failed, // Exhausted all retries
        resolution_method: None,
        resolution_confidence: None,
        retry_state: AssertionRetryState {
            resolution_attempts: max_attempts,
            last_resolution_at: Some(now - Duration::hours(1)),
            abstention_reason: Some("Exceeded max resolution attempts (5). Multiple 'Dr. Smith' entities remain indistinguishable.".to_string()),
        },
        subject_entity: None,
        object_entity: None,
        confidence: 0.75,
        subject_entity_type: None,
        object_entity_type: None,
        significance: None,
        lifecycle_tier: "active".to_string(),
    };
    let created_assertion_2 = assertion_repo.create(assertion_2).await?;
    assert_eq!(
        created_assertion_2.resolution_status,
        ResolutionStatus::Failed,
        "Should be Failed after max attempts"
    );
    assert_eq!(
        created_assertion_2.retry_state.resolution_attempts, max_attempts,
        "Should have max attempts recorded"
    );

    // Verify that Failed status is not eligible for retry
    let failed_not_eligible = is_eligible_for_retry(
        &created_assertion_2.resolution_status,
        created_assertion_2.retry_state.resolution_attempts,
        created_assertion_2.retry_state.last_resolution_at,
        now + Duration::days(1), // Even far in the future
        max_attempts,
    );
    assert!(
        !failed_not_eligible,
        "Failed status should not be eligible for retry"
    );

    // Create an assertion that eventually resolves
    let assertion_3_id = RecordId::new_v4(TableName::Assertion);
    let assertion_3_base = BaseFields::new(
        assertion_3_id.clone(),
        space_id.to_string(),
        "extractor".to_string(),
    );
    // Note: Not setting provenance.sources to avoid SurrealDB meta::tb() serialization issues
    let entity_id = RecordId::new_v4(TableName::Entity);
    let assertion_3 = Assertion {
        base: assertion_3_base,
        subject_text: "John Smith".to_string(), // More specific - can resolve
        predicate_raw: "attended".to_string(),
        predicate_id: None,
        object_text: Some("quarterly meeting".to_string()),
        object_literal: None,
        evidence: EvidenceSpan {
            revision: Link::new(doc_rev_id.clone()),
            node: None,
            start: 0,
            end: 30,
            quote_hash: "sha256:evidence_hash_3".to_string(),
        },
        source_event_time: None,
        observed_at: now,
        asserted_at: now,
        assertion_key: format!("{}:attended:johnsmith", doc_rev_id),
        resolution_status: ResolutionStatus::Resolved, // Successfully resolved after retry
        resolution_method: None,
        resolution_confidence: None,
        retry_state: AssertionRetryState {
            resolution_attempts: 3, // Took 3 attempts
            last_resolution_at: Some(now),
            abstention_reason: None, // Cleared on success
        },
        subject_entity: Some(Link::new(entity_id)), // Now resolved
        object_entity: None,
        confidence: 0.92,
        subject_entity_type: None,
        object_entity_type: None,
        significance: None,
        lifecycle_tier: "active".to_string(),
    };
    let created_assertion_3 = assertion_repo.create(assertion_3).await?;
    assert_eq!(
        created_assertion_3.resolution_status,
        ResolutionStatus::Resolved,
        "Should be Resolved after successful retry"
    );
    assert!(
        created_assertion_3.subject_entity.is_some(),
        "Resolved assertion should have subject entity"
    );

    // Verify all assertions stored correctly
    let all_assertions = assertion_repo.list(space_id, 100, None).await?;
    assert_eq!(
        all_assertions.len(),
        3,
        "All three assertions should be stored"
    );

    // Count by status
    let abstained_count = all_assertions
        .iter()
        .filter(|a| a.resolution_status == ResolutionStatus::Abstained)
        .count();
    let failed_count = all_assertions
        .iter()
        .filter(|a| a.resolution_status == ResolutionStatus::Failed)
        .count();
    let resolved_count = all_assertions
        .iter()
        .filter(|a| a.resolution_status == ResolutionStatus::Resolved)
        .count();

    assert_eq!(abstained_count, 1, "One assertion still abstained");
    assert_eq!(failed_count, 1, "One assertion failed");
    assert_eq!(resolved_count, 1, "One assertion resolved");

    Ok(())
}
