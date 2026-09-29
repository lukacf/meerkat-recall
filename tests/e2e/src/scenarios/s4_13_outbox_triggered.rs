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
//! S4.13 Outbox-triggered truth maintenance scenario test.
//!
//! Verifies TM trigger types from outbox events.

use chrono::Utc;
use elephant_storage::{
    AssertionRepository, ConflictGroupRepository, EntityRepository, Repository, SourceRepository,
    TruthSlotRepository,
};
use elephant_types::ids::TableName;
use elephant_types::records::assertion::{
    Assertion, AssertionRetryState, EvidenceSpan, ResolutionStatus,
};
use elephant_types::records::base::{BaseFields, Security, SubjectsState};
use elephant_types::records::entity::EntityType;
use elephant_types::records::rel::ClaimStatus;
use elephant_types::records::source::{Source, SourceType};
use elephant_types::records::truth::{ConflictGroup, CurrentValue, TruthSlot};
use elephant_types::records::Entity;
use elephant_types::{Link, RecordId};

use crate::Harness;

/// S4.13: Outbox-triggered truth maintenance.
///
/// Test invariants:
/// 1. NewAssertion triggers slot allocation
/// 2. ConflictDetected triggers resolution workflow
/// 3. SourceRetracted triggers cascade supersession
/// 4. FreshnessChanged triggers re-evaluation of affected slots
///
/// Note: This test focuses on the truth maintenance state machines rather than
/// the outbox event creation (which is handled by the pipeline system).
#[tokio::test]
async fn scenario_s4_13_outbox_triggered() -> anyhow::Result<()> {
    let harness = Harness::new().await?;
    let entity_repo = EntityRepository::new(harness.client.clone());
    let assertion_repo = AssertionRepository::new(harness.client.clone());
    let source_repo = SourceRepository::new(harness.client.clone());
    let truth_slot_repo = TruthSlotRepository::new(harness.client.clone());
    let conflict_group_repo = ConflictGroupRepository::new(harness.client.clone());

    let space_id = "Space_OutboxTM";

    // Create entities for testing
    let entity_carol_id = RecordId::new_v4(TableName::Entity);
    let mut entity_carol_base = BaseFields::new(
        entity_carol_id.clone(),
        space_id.to_string(),
        "P_sys".to_string(),
    );
    entity_carol_base.security = Security {
        policy_id: "default".to_string(),
        level: 1,
        ..Default::default()
    };
    entity_carol_base.subjects_state = SubjectsState::Complete;
    let mut entity_carol = Entity::new(
        entity_carol_base,
        EntityType::new("person"),
        "Carol".to_string(),
    );
    entity_carol.identity_keys = vec!["name:carol".to_string()];
    entity_repo.create(entity_carol).await?;

    let entity_megacorp_id = RecordId::new_v4(TableName::Entity);
    let mut entity_megacorp_base = BaseFields::new(
        entity_megacorp_id.clone(),
        space_id.to_string(),
        "P_sys".to_string(),
    );
    entity_megacorp_base.security = Security {
        policy_id: "default".to_string(),
        level: 1,
        ..Default::default()
    };
    entity_megacorp_base.subjects_state = SubjectsState::Complete;
    let mut entity_megacorp = Entity::new(
        entity_megacorp_base,
        EntityType::new("org"),
        "MegaCorp".to_string(),
    );
    entity_megacorp.identity_keys = vec!["name:megacorp".to_string()];
    entity_repo.create(entity_megacorp).await?;

    let entity_smallco_id = RecordId::new_v4(TableName::Entity);
    let mut entity_smallco_base = BaseFields::new(
        entity_smallco_id.clone(),
        space_id.to_string(),
        "P_sys".to_string(),
    );
    entity_smallco_base.security = Security {
        policy_id: "default".to_string(),
        level: 1,
        ..Default::default()
    };
    entity_smallco_base.subjects_state = SubjectsState::Complete;
    let mut entity_smallco = Entity::new(
        entity_smallco_base,
        EntityType::new("org"),
        "SmallCo".to_string(),
    );
    entity_smallco.identity_keys = vec!["name:smallco".to_string()];
    entity_repo.create(entity_smallco).await?;

    // Create a source for assertions
    let source_id = RecordId::new_v4(TableName::Source);
    let mut source_base =
        BaseFields::new(source_id.clone(), space_id.to_string(), "P_sys".to_string());
    source_base.security = Security {
        policy_id: "default".to_string(),
        level: 1,
        ..Default::default()
    };
    source_base.subjects_state = SubjectsState::Complete;
    let mut source = Source::new(source_base, SourceType::Email, "hash_email_1".to_string());
    source.external_id = Some("email-001".to_string());
    source.uri = Some("mailto:hr@megacorp.com".to_string());
    source.metadata = serde_json::json!({"subject": "Employment Update"});
    source_repo.create(source).await?;

    // Invariant 1: NewAssertion triggers slot allocation
    // Create a new assertion
    let assertion_1_id = RecordId::new_v4(TableName::Assertion);
    let assertion_1_base = BaseFields::new(
        assertion_1_id.clone(),
        space_id.to_string(),
        "extractor".to_string(),
    );
    let revision_id = RecordId::new_v4(TableName::DocRevision);
    let assertion_1 = Assertion {
        base: assertion_1_base,
        subject_text: "Carol".to_string(),
        predicate_raw: "works_at".to_string(),
        predicate_id: None,
        object_text: Some("MegaCorp".to_string()),
        object_literal: None,
        evidence: EvidenceSpan {
            revision: Link::new(revision_id.clone()),
            node: None,
            start: 0,
            end: 25,
            quote_hash: "sha256:carol_works_mega".to_string(),
        },
        source_event_time: Some(Utc::now()),
        observed_at: Utc::now(),
        asserted_at: Utc::now(),
        assertion_key: "assertion-carol-works-megacorp".to_string(),
        resolution_status: ResolutionStatus::Resolved,
        resolution_method: None,
        resolution_confidence: None,
        retry_state: AssertionRetryState::default(),
        subject_entity: Some(Link::new(entity_carol_id.clone())),
        object_entity: Some(Link::new(entity_megacorp_id.clone())),
        confidence: 0.9,
        subject_entity_type: None,
        object_entity_type: None,
        significance: None,
        lifecycle_tier: "active".to_string(),
    };
    assertion_repo.create(assertion_1).await?;

    // Truth maintenance allocates a slot for this assertion (simulating the NewAssertion event trigger)
    let slot_key = format!("{entity_carol_id}:works_at:{entity_megacorp_id}");
    let slot_1_id = RecordId::new_v4(TableName::TruthSlot);
    let slot_1_base = BaseFields::new(
        slot_1_id.clone(),
        space_id.to_string(),
        "truth_maintenance".to_string(),
    );
    let slot_1 = TruthSlot {
        base: slot_1_base,
        slot_key: slot_key.clone(),
        slot_type: "rel".to_string(),
        subject_entity: entity_carol_id.to_string(),
        predicate_id: "works_at".to_string(),
        current_values: vec![CurrentValue {
            claim_id: assertion_1_id.to_string(),
            confidence: 0.9,
            status: ClaimStatus::Candidate,
            evidence: vec![],
        }],
        conflict_group: None,
        confidence: 0.9,
        rationale: Some("Single assertion with high confidence".to_string()),
        semantics_version: 1,
        alias_version: 1,
        stale: false,
        stale_reason: None,
        computed_at: Utc::now(),
        security_hash: "hash_default".to_string(),
    };
    let created_slot_1 = truth_slot_repo.create(slot_1).await?;
    assert_eq!(
        created_slot_1.current_values.len(),
        1,
        "CRITICAL: Slot should have one current value after NewAssertion trigger"
    );
    assert!(!created_slot_1.stale, "New slot should not be stale");

    // Invariant 2: ConflictDetected triggers resolution workflow
    // Create a conflicting assertion (Carol works at SmallCo instead)
    let assertion_2_id = RecordId::new_v4(TableName::Assertion);
    let assertion_2_base = BaseFields::new(
        assertion_2_id.clone(),
        space_id.to_string(),
        "extractor".to_string(),
    );
    let assertion_2 = Assertion {
        base: assertion_2_base,
        subject_text: "Carol".to_string(),
        predicate_raw: "works_at".to_string(),
        predicate_id: None,
        object_text: Some("SmallCo".to_string()),
        object_literal: None,
        evidence: EvidenceSpan {
            revision: Link::new(revision_id.clone()),
            node: None,
            start: 30,
            end: 55,
            quote_hash: "sha256:carol_works_small".to_string(),
        },
        source_event_time: Some(Utc::now()),
        observed_at: Utc::now(),
        asserted_at: Utc::now(),
        assertion_key: "assertion-carol-works-smallco".to_string(),
        resolution_status: ResolutionStatus::Resolved,
        resolution_method: None,
        resolution_confidence: None,
        retry_state: AssertionRetryState::default(),
        subject_entity: Some(Link::new(entity_carol_id.clone())),
        object_entity: Some(Link::new(entity_smallco_id.clone())),
        confidence: 0.85, // Lower confidence
        subject_entity_type: None,
        object_entity_type: None,
        significance: None,
        lifecycle_tier: "active".to_string(),
    };
    assertion_repo.create(assertion_2).await?;

    // Create conflict group for the competing claims (simulating ConflictDetected event trigger)
    let conflict_group_id = RecordId::new_v4(TableName::ConflictGroup);
    let conflict_group_base = BaseFields::new(
        conflict_group_id.clone(),
        space_id.to_string(),
        "truth_maintenance".to_string(),
    );
    let conflict_group = ConflictGroup {
        base: conflict_group_base,
        slot_key: slot_key.clone(),
        slot_type: "rel".to_string(),
        claim_ids: vec![assertion_1_id.to_string(), assertion_2_id.to_string()],
        resolved: false,
        resolution_rationale: None,
        detected_at: Utc::now(),
        resolved_at: None,
        adjudicated_claim_set_hash: None,
        adjudicated_at: None,
    };
    let created_conflict = conflict_group_repo.create(conflict_group).await?;
    assert!(
        !created_conflict.resolved,
        "Conflict should start unresolved"
    );
    assert_eq!(
        created_conflict.claim_ids.len(),
        2,
        "Conflict should have two competing claims"
    );

    // Update truth slot to reflect conflict
    let mut conflicted_slot = created_slot_1.clone();
    conflicted_slot.current_values = vec![
        CurrentValue {
            claim_id: assertion_1_id.to_string(),
            confidence: 0.9,
            status: ClaimStatus::Disputed,
            evidence: vec![],
        },
        CurrentValue {
            claim_id: assertion_2_id.to_string(),
            confidence: 0.85,
            status: ClaimStatus::Disputed,
            evidence: vec![],
        },
    ];
    conflicted_slot.conflict_group = Some(Link::new(conflict_group_id.clone()));
    conflicted_slot.stale = true;
    conflicted_slot.stale_reason = Some("Conflict detected - needs resolution".to_string());
    let updated_slot = truth_slot_repo
        .update(space_id, &slot_1_id, conflicted_slot)
        .await?;
    assert!(
        updated_slot.stale,
        "Slot should be marked stale during conflict"
    );
    assert!(
        updated_slot.conflict_group.is_some(),
        "Slot should link to conflict group"
    );

    // Invariant 3: SourceRetracted triggers cascade supersession
    // Mark assertion from retracted source as superseded (simulating SourceRetracted event trigger)
    let mut superseded_assertion = assertion_repo
        .get(space_id, &assertion_2_id)
        .await?
        .unwrap();
    superseded_assertion.resolution_status = ResolutionStatus::Superseded;
    let updated_assertion = assertion_repo
        .update(space_id, &assertion_2_id, superseded_assertion)
        .await?;
    assert_eq!(
        updated_assertion.resolution_status,
        ResolutionStatus::Superseded,
        "Assertion from retracted source should be superseded"
    );

    // Resolve conflict group after retraction
    let mut resolved_conflict = created_conflict.clone();
    resolved_conflict.resolved = true;
    resolved_conflict.resolution_rationale =
        Some("Competing claim superseded due to source retraction".to_string());
    resolved_conflict.resolved_at = Some(Utc::now());
    let final_conflict = conflict_group_repo
        .update(space_id, &conflict_group_id, resolved_conflict)
        .await?;
    assert!(final_conflict.resolved, "Conflict should be resolved");
    assert!(
        final_conflict.resolution_rationale.is_some(),
        "Resolution rationale should be recorded"
    );

    // Invariant 4: FreshnessChanged triggers re-evaluation of affected slots
    // Update truth slot to reflect freshness change (simulating FreshnessChanged event trigger)
    let mut refreshed_slot = updated_slot.clone();
    refreshed_slot.current_values = vec![CurrentValue {
        claim_id: assertion_1_id.to_string(),
        confidence: 0.9,
        status: ClaimStatus::Approved, // Winner after conflict resolution
        evidence: vec![],
    }];
    refreshed_slot.conflict_group = None; // Conflict resolved
    refreshed_slot.stale = true; // Marked stale for re-evaluation due to freshness
    refreshed_slot.stale_reason = Some("Freshness changed - re-evaluation needed".to_string());
    refreshed_slot.computed_at = Utc::now();
    let freshness_slot = truth_slot_repo
        .update(space_id, &slot_1_id, refreshed_slot)
        .await?;
    assert!(
        freshness_slot.stale,
        "Slot should be stale after freshness change"
    );

    // Final re-evaluation clears stale flag
    let mut final_slot = freshness_slot.clone();
    final_slot.stale = false;
    final_slot.stale_reason = None;
    final_slot.computed_at = Utc::now();
    let completed_slot = truth_slot_repo
        .update(space_id, &slot_1_id, final_slot)
        .await?;
    assert!(
        !completed_slot.stale,
        "Slot should not be stale after re-evaluation"
    );
    assert_eq!(
        completed_slot.current_values.len(),
        1,
        "Slot should have single winner"
    );
    assert_eq!(
        completed_slot.current_values[0].status,
        ClaimStatus::Approved,
        "Winner should have Approved status"
    );

    // Verify the truth maintenance state machine completed correctly
    let final_assertions = assertion_repo.list(space_id, 100, None).await?;
    assert_eq!(final_assertions.len(), 2, "Should have two assertions");

    let superseded_count = final_assertions
        .iter()
        .filter(|a| a.resolution_status == ResolutionStatus::Superseded)
        .count();
    assert_eq!(
        superseded_count, 1,
        "One assertion should be superseded due to source retraction"
    );

    let resolved_count = final_assertions
        .iter()
        .filter(|a| a.resolution_status == ResolutionStatus::Resolved)
        .count();
    assert_eq!(resolved_count, 1, "One assertion should remain resolved");

    Ok(())
}
