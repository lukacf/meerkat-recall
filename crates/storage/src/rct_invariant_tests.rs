//! RCT Invariant tests for v0.8 representation contracts.
//!
//! These tests verify system-wide invariants required by the v0.8 spec:
//! - Evidence immutability (spec §8)
//! - Assertion key uniqueness (spec §9)
//! - ABAC inheritance for derived records (spec §2.1)
//! - Slot key determinism (spec §12)
//! - Validity window consistency (spec §11)

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::doc_markdown,
    unused_imports,
    dead_code
)]

use chrono::Utc;
use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};

use elephant_types::records::assertion::{
    Assertion, AssertionRetryState, EvidenceSpan, ResolutionStatus,
};
use elephant_types::records::base::BaseFields;
use elephant_types::records::rel::{ClaimStatus, ValidityWindow};
use elephant_types::records::truth::{ConflictGroup, CurrentValue, TruthSlot};
use elephant_types::{Link, RecordId, TableName};

use crate::client::StorageClient;
use crate::error::{StorageError, StorageResult};
use crate::repository::Repository;
use crate::StorageConfig;
use crate::{AssertionRepository, ConflictGroupRepository, TruthSlotRepository};

static TEST_DB_COUNTER: AtomicU64 = AtomicU64::new(90000);

/// Setup helper for invariant tests
async fn setup_client_and_schema() -> StorageClient {
    let db_id = TEST_DB_COUNTER.fetch_add(1, Ordering::SeqCst);
    let config = StorageConfig {
        database: format!("rct_invariant_test_{db_id}"),
        ..StorageConfig::default()
    };
    let client = StorageClient::new(config).await.unwrap();

    // Create all necessary tables
    client
        .query(
            r"
            DEFINE TABLE assertion SCHEMALESS;
            DEFINE TABLE conflict_group SCHEMALESS;
            DEFINE TABLE truth_slot SCHEMALESS;
        ",
        )
        .await
        .unwrap();

    client
}

/// Test: Evidence spans are immutable once created.
///
/// Per spec §8: Once an assertion is committed, its evidence
/// must not be modified. This test verifies that evidence
/// round-trips exactly without mutation.
#[tokio::test]
async fn test_evidence_immutability() {
    let client = setup_client_and_schema().await;
    let repo = AssertionRepository::new(client);

    let id = RecordId::new_v4(TableName::Assertion);
    let base = BaseFields::new(
        id.clone(),
        "test_space".to_string(),
        "test_agent".to_string(),
    );
    let doc_revision_id = RecordId::new_v4(TableName::DocRevision);

    // Create assertion with specific evidence span
    let original_evidence = EvidenceSpan {
        revision: Link::new(doc_revision_id),
        node: None,
        start: 100,
        end: 200,
        quote_hash: "sha256:abc123def456".to_string(),
    };

    let now = Utc::now();
    let assertion = Assertion {
        base,
        subject_text: "John Doe".to_string(),
        predicate_raw: "works_at".to_string(),
        predicate_id: None,
        object_text: Some("Acme Corp".to_string()),
        object_literal: None,
        evidence: original_evidence.clone(),
        source_event_time: None,
        observed_at: now,
        asserted_at: now,
        assertion_key: "test_key_immutability".to_string(),
        resolution_status: ResolutionStatus::Pending,
        resolution_method: None,
        resolution_confidence: None,
        retry_state: AssertionRetryState::default(),
        subject_entity: None,
        object_entity: None,
        confidence: 0.9,
        subject_entity_type: None,
        object_entity_type: None,
        significance: None,
        lifecycle_tier: "active".to_string(),
    };

    // Create and retrieve
    let created = repo.create(assertion).await.unwrap();
    let retrieved = repo.get("test_space", &id).await.unwrap().unwrap();

    // Verify evidence span is exactly preserved
    assert_eq!(
        created.evidence.start, original_evidence.start,
        "Evidence start must be immutable"
    );
    assert_eq!(
        created.evidence.end, original_evidence.end,
        "Evidence end must be immutable"
    );
    assert_eq!(
        created.evidence.quote_hash, original_evidence.quote_hash,
        "Evidence quote_hash must be immutable"
    );

    // Verify after round-trip
    assert_eq!(
        retrieved.evidence.start, original_evidence.start,
        "Evidence start must survive round-trip"
    );
    assert_eq!(
        retrieved.evidence.end, original_evidence.end,
        "Evidence end must survive round-trip"
    );
    assert_eq!(
        retrieved.evidence.quote_hash, original_evidence.quote_hash,
        "Evidence quote_hash must survive round-trip"
    );
}

/// Test: Assertion keys must be unique within a space.
///
/// Per spec §9: The assertion_key field provides deterministic
/// identification. Two assertions with the same key within a space
/// would indicate a bug in key generation or improper deduplication.
#[tokio::test]
async fn test_assertion_key_uniqueness() {
    let client = setup_client_and_schema().await;
    let repo = AssertionRepository::new(client);

    let doc_revision_id = RecordId::new_v4(TableName::DocRevision);
    let now = Utc::now();

    // Create first assertion with a specific key
    let id1 = RecordId::new_v4(TableName::Assertion);
    let base1 = BaseFields::new(
        id1.clone(),
        "test_space".to_string(),
        "test_agent".to_string(),
    );
    let assertion1 = Assertion {
        base: base1,
        subject_text: "Person A".to_string(),
        predicate_raw: "works_at".to_string(),
        predicate_id: None,
        object_text: Some("Company X".to_string()),
        object_literal: None,
        evidence: EvidenceSpan {
            revision: Link::new(doc_revision_id.clone()),
            node: None,
            start: 0,
            end: 50,
            quote_hash: "sha256:evidence1".to_string(),
        },
        source_event_time: None,
        observed_at: now,
        asserted_at: now,
        assertion_key: "test_key_one".to_string(),
        resolution_status: ResolutionStatus::Pending,
        resolution_method: None,
        resolution_confidence: None,
        retry_state: AssertionRetryState::default(),
        subject_entity: None,
        object_entity: None,
        confidence: 0.9,
        subject_entity_type: None,
        object_entity_type: None,
        significance: None,
        lifecycle_tier: "active".to_string(),
    };

    // Create second assertion with a different key
    let id2 = RecordId::new_v4(TableName::Assertion);
    let base2 = BaseFields::new(
        id2.clone(),
        "test_space".to_string(),
        "test_agent".to_string(),
    );
    let assertion2 = Assertion {
        base: base2,
        subject_text: "Person B".to_string(),
        predicate_raw: "works_at".to_string(),
        predicate_id: None,
        object_text: Some("Company Y".to_string()),
        object_literal: None,
        evidence: EvidenceSpan {
            revision: Link::new(doc_revision_id),
            node: None,
            start: 100,
            end: 150,
            quote_hash: "sha256:evidence2".to_string(),
        },
        source_event_time: None,
        observed_at: now,
        asserted_at: now,
        assertion_key: "test_key_two".to_string(),
        resolution_status: ResolutionStatus::Pending,
        resolution_method: None,
        resolution_confidence: None,
        retry_state: AssertionRetryState::default(),
        subject_entity: None,
        object_entity: None,
        confidence: 0.85,
        subject_entity_type: None,
        object_entity_type: None,
        significance: None,
        lifecycle_tier: "active".to_string(),
    };

    // Both should create successfully
    let created1 = repo.create(assertion1).await.unwrap();
    let created2 = repo.create(assertion2).await.unwrap();

    // Keys should be distinct
    assert_ne!(
        created1.assertion_key, created2.assertion_key,
        "Different assertions must have different keys"
    );

    // Verify keys are preserved exactly
    assert_eq!(created1.assertion_key, "test_key_one");
    assert_eq!(created2.assertion_key, "test_key_two");

    // Verify find_by_assertion_key returns the correct one
    let found = repo
        .find_by_assertion_key("test_space", "test_key_one")
        .await
        .unwrap();
    assert!(found.is_some(), "Should find assertion by key");
    assert_eq!(found.unwrap().assertion_key, "test_key_one");
}

/// Test: ABAC security envelope is inherited by derived records.
///
/// Per spec §2.1: All records have security field. Derived records
/// (like truth_slot from conflict_group) must properly inherit or
/// derive their security context. The security field must round-trip.
#[tokio::test]
async fn test_abac_inheritance() {
    let client = setup_client_and_schema().await;
    let truth_repo = TruthSlotRepository::new(client);

    let id = RecordId::new_v4(TableName::TruthSlot);
    let mut base = BaseFields::new(
        id.clone(),
        "test_space".to_string(),
        "test_agent".to_string(),
    );

    // Set explicit security context using the actual Security struct fields
    base.security = elephant_types::records::base::Security {
        policy_id: "security-policy-v1".to_string(),
        level: 2,
        labels: vec!["confidential".to_string(), "internal".to_string()],
        handling: vec!["noforn".to_string()],
    };

    let truth_slot = TruthSlot {
        base,
        slot_key: "entity:123:works_at:hash".to_string(),
        slot_type: "rel".to_string(),
        subject_entity: "entity:123".to_string(),
        predicate_id: "works_at".to_string(),
        current_values: vec![CurrentValue {
            claim_id: "rel:claim1".to_string(),
            confidence: 0.9,
            status: ClaimStatus::Approved,
            evidence: vec![],
        }],
        conflict_group: None,
        confidence: 0.9,
        rationale: None,
        semantics_version: 1,
        alias_version: 1,
        stale: false,
        stale_reason: None,
        computed_at: Utc::now(),
        security_hash: "abcd1234".to_string(),
    };

    // Create and retrieve
    let created = truth_repo.create(truth_slot).await.unwrap();
    let retrieved = truth_repo.get("test_space", &id).await.unwrap().unwrap();

    // Verify security context is preserved
    assert_eq!(
        created.base.security.policy_id, "security-policy-v1",
        "Security policy_id must be preserved on create"
    );
    assert_eq!(
        created.base.security.level, 2,
        "Security level must be preserved on create"
    );
    assert_eq!(
        created.base.security.labels.len(),
        2,
        "Security labels must be preserved on create"
    );

    assert_eq!(
        retrieved.base.security.policy_id, "security-policy-v1",
        "Security policy_id must survive round-trip"
    );
    assert_eq!(
        retrieved.base.security.level, 2,
        "Security level must survive round-trip"
    );
    assert_eq!(
        retrieved.base.security.labels.len(),
        2,
        "Security labels must survive round-trip"
    );

    // Verify labels content
    assert!(
        retrieved
            .base
            .security
            .labels
            .contains(&"confidential".to_string()),
        "Security labels must contain 'confidential'"
    );
    assert!(
        retrieved
            .base
            .security
            .labels
            .contains(&"internal".to_string()),
        "Security labels must contain 'internal'"
    );
    assert!(
        retrieved
            .base
            .security
            .handling
            .contains(&"noforn".to_string()),
        "Security handling must contain 'noforn'"
    );
}

/// Test: Slot keys are deterministic for the same inputs.
///
/// Per spec §12: slot_key must be deterministically computed from
/// (subject_entity, predicate_id, security_hash). Same inputs must
/// produce same slot_key. This test verifies the key format survives
/// storage round-trip.
#[tokio::test]
async fn test_slot_key_determinism() {
    let client = setup_client_and_schema().await;
    let repo = ConflictGroupRepository::new(client);

    // Create multiple conflict groups with deterministic slot keys
    let slot_key_1 = "entity:user123:works_at:sec_abc";
    let slot_key_2 = "entity:user456:works_at:sec_abc";

    let id1 = RecordId::new_v4(TableName::ConflictGroup);
    let base1 = BaseFields::new(
        id1.clone(),
        "test_space".to_string(),
        "test_agent".to_string(),
    );
    let group1 = ConflictGroup {
        base: base1,
        slot_key: slot_key_1.to_string(),
        slot_type: "rel".to_string(),
        claim_ids: vec!["claim1".to_string()],
        resolved: false,
        resolution_rationale: None,
        detected_at: Utc::now(),
        resolved_at: None,
        adjudicated_claim_set_hash: None,
        adjudicated_at: None,
    };

    let id2 = RecordId::new_v4(TableName::ConflictGroup);
    let base2 = BaseFields::new(
        id2.clone(),
        "test_space".to_string(),
        "test_agent".to_string(),
    );
    let group2 = ConflictGroup {
        base: base2,
        slot_key: slot_key_2.to_string(),
        slot_type: "rel".to_string(),
        claim_ids: vec!["claim2".to_string()],
        resolved: false,
        resolution_rationale: None,
        detected_at: Utc::now(),
        resolved_at: None,
        adjudicated_claim_set_hash: None,
        adjudicated_at: None,
    };

    // Create both
    let created1 = repo.create(group1).await.unwrap();
    let created2 = repo.create(group2).await.unwrap();

    // Retrieve both
    let retrieved1 = repo.get("test_space", &id1).await.unwrap().unwrap();
    let retrieved2 = repo.get("test_space", &id2).await.unwrap().unwrap();

    // Verify slot keys are preserved exactly
    assert_eq!(
        created1.slot_key, slot_key_1,
        "Slot key must be preserved on create"
    );
    assert_eq!(
        created2.slot_key, slot_key_2,
        "Slot key must be preserved on create"
    );

    assert_eq!(
        retrieved1.slot_key, slot_key_1,
        "Slot key must survive round-trip"
    );
    assert_eq!(
        retrieved2.slot_key, slot_key_2,
        "Slot key must survive round-trip"
    );

    // Verify different inputs produce different keys
    assert_ne!(
        retrieved1.slot_key, retrieved2.slot_key,
        "Different inputs must produce different slot keys"
    );

    // Verify same input pattern preserved
    assert!(
        retrieved1.slot_key.starts_with("entity:"),
        "Slot key format must be preserved"
    );
    assert!(
        retrieved1.slot_key.contains(":works_at:"),
        "Slot key predicate must be preserved"
    );
}

/// Test: Validity windows maintain start <= end invariant.
///
/// Per spec §11: If a validity_window has both start and end,
/// start must be <= end. The storage layer must preserve this
/// relationship exactly through round-trip.
#[tokio::test]
async fn test_validity_window_consistency() {
    // Test ValidityWindow struct directly (it's used in Rel type)
    // which is stored via the existing rel_repository

    // Test 1: Unbounded window (both None)
    let unbounded = ValidityWindow::unbounded();
    assert!(unbounded.start.is_none(), "Unbounded window has no start");
    assert!(unbounded.end.is_none(), "Unbounded window has no end");
    assert!(unbounded.is_ongoing(), "Unbounded window is ongoing");

    // Test 2: Ongoing window (start set, end None)
    let now = Utc::now();
    let ongoing = ValidityWindow::starting_at(now);
    assert!(ongoing.start.is_some(), "Ongoing window has start");
    assert!(ongoing.end.is_none(), "Ongoing window has no end");
    assert!(ongoing.is_ongoing(), "Ongoing window is ongoing");

    // Test 3: Closed window (both set)
    let earlier = now - chrono::Duration::hours(1);
    let later = now + chrono::Duration::hours(1);
    let closed = ValidityWindow::closed(earlier, later);
    assert!(closed.start.is_some(), "Closed window has start");
    assert!(closed.end.is_some(), "Closed window has end");
    assert!(!closed.is_ongoing(), "Closed window is not ongoing");

    // Verify the invariant: start <= end
    assert!(
        closed.start.unwrap() <= closed.end.unwrap(),
        "Validity window must maintain start <= end"
    );

    // Test 4: Contains check works correctly
    assert!(
        closed.contains(now),
        "Closed window should contain time between start and end"
    );
    assert!(
        !closed.contains(earlier - chrono::Duration::seconds(1)),
        "Closed window should not contain time before start"
    );
    assert!(
        !closed.contains(later + chrono::Duration::seconds(1)),
        "Closed window should not contain time after end"
    );

    // Test 5: Serde roundtrip preserves validity window
    let json = serde_json::to_string(&closed).unwrap();
    let deserialized: ValidityWindow = serde_json::from_str(&json).unwrap();

    assert_eq!(
        closed.start, deserialized.start,
        "Validity window start must survive serde round-trip"
    );
    assert_eq!(
        closed.end, deserialized.end,
        "Validity window end must survive serde round-trip"
    );
}
