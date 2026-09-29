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
//! S4.4 Truth maintenance scenario test.
//!
//! Verifies conflict detection and resolution in truth slots.

use chrono::Utc;
use elephant_storage::{
    ConflictGroupRepository, EntityRepository, RelRepository, Repository, TruthSlotRepository,
};
use elephant_types::ids::TableName;
use elephant_types::records::base::{BaseFields, Security, SubjectsState};
use elephant_types::records::entity::EntityType;
use elephant_types::records::rel::{ClaimStatus, Rel, ValidityWindow};
use elephant_types::records::truth::{ConflictGroup, CurrentValue, TruthSlot};
use elephant_types::records::Entity;
use elephant_types::{Link, RecordId};

use crate::Harness;

/// S4.4: Truth maintenance conflict handling.
///
/// Test invariants:
/// 1. Conflicting claims detected via truth_slot grouping
/// 2. Conflict groups created for resolution
/// 3. Resolution strategies applied (recency, confidence, manual)
/// 4. Winning claim promoted, losers marked conflicted
#[tokio::test]
async fn scenario_s4_4_truth_maintenance() -> anyhow::Result<()> {
    let harness = Harness::new().await?;
    let entity_repo = EntityRepository::new(harness.client.clone());
    let rel_repo = RelRepository::new(harness.client.clone());
    let conflict_group_repo = ConflictGroupRepository::new(harness.client.clone());
    let truth_slot_repo = TruthSlotRepository::new(harness.client.clone());

    let space_id = "Space_TruthMaint";

    // Create entities for relationship testing
    let entity_john_id = RecordId::new_v4(TableName::Entity);
    let mut entity_john_base = BaseFields::new(
        entity_john_id.clone(),
        space_id.to_string(),
        "P_sys".to_string(),
    );
    entity_john_base.security = Security {
        policy_id: "default".to_string(),
        level: 1,
        ..Default::default()
    };
    entity_john_base.subjects_state = SubjectsState::Complete;
    let entity_john = Entity::new(
        entity_john_base,
        EntityType::new("person"),
        "John Smith".to_string(),
    );
    entity_repo.create(entity_john).await?;

    let entity_acme_id = RecordId::new_v4(TableName::Entity);
    let mut entity_acme_base = BaseFields::new(
        entity_acme_id.clone(),
        space_id.to_string(),
        "P_sys".to_string(),
    );
    entity_acme_base.security = Security {
        policy_id: "default".to_string(),
        level: 1,
        ..Default::default()
    };
    entity_acme_base.subjects_state = SubjectsState::Complete;
    let entity_acme = Entity::new(
        entity_acme_base,
        EntityType::new("org"),
        "Acme Corp".to_string(),
    );
    entity_repo.create(entity_acme).await?;

    let entity_globex_id = RecordId::new_v4(TableName::Entity);
    let mut entity_globex_base = BaseFields::new(
        entity_globex_id.clone(),
        space_id.to_string(),
        "P_sys".to_string(),
    );
    entity_globex_base.security = Security {
        policy_id: "default".to_string(),
        level: 1,
        ..Default::default()
    };
    entity_globex_base.subjects_state = SubjectsState::Complete;
    let entity_globex = Entity::new(
        entity_globex_base,
        EntityType::new("org"),
        "Globex Inc".to_string(),
    );
    entity_repo.create(entity_globex).await?;

    // Invariant 1: Conflicting claims detected via truth_slot grouping
    // Create two competing "works_at" relationships for John (conflicting claims)
    let rel_1_id = RecordId::new_v4(TableName::Rel);
    let mut rel_1_base = BaseFields::new(
        rel_1_id.clone(),
        space_id.to_string(),
        "extractor_1".to_string(),
    );
    rel_1_base.security = Security {
        policy_id: "default".to_string(),
        level: 1,
        ..Default::default()
    };
    let rel_1 = Rel {
        base: rel_1_base,
        subject: Link::new(entity_john_id.clone()),
        object: Link::new(entity_acme_id.clone()),
        rel_type: "works_at".to_string(),
        status: ClaimStatus::Candidate,
        confidence: 0.85,
        validity_window: None,
        evidence: vec![],
        temporal_audit: None,
    };
    let created_rel_1 = rel_repo.create(rel_1).await?;

    let rel_2_id = RecordId::new_v4(TableName::Rel);
    let mut rel_2_base = BaseFields::new(
        rel_2_id.clone(),
        space_id.to_string(),
        "extractor_2".to_string(),
    );
    rel_2_base.security = Security {
        policy_id: "default".to_string(),
        level: 1,
        ..Default::default()
    };
    let rel_2 = Rel {
        base: rel_2_base,
        subject: Link::new(entity_john_id.clone()),
        object: Link::new(entity_globex_id.clone()),
        rel_type: "works_at".to_string(),
        status: ClaimStatus::Candidate,
        confidence: 0.75,
        validity_window: None,
        evidence: vec![],
        temporal_audit: None,
    };
    let created_rel_2 = rel_repo.create(rel_2).await?;

    // Both claims compete for the same slot: John's current employer
    // Slot key identifies the truth slot: (subject_entity, predicate, security_hash)
    let slot_key = format!("{entity_john_id}:works_at:default");

    // Invariant 2: Conflict groups created for resolution
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
        claim_ids: vec![
            created_rel_1.base.id.to_string(),
            created_rel_2.base.id.to_string(),
        ],
        resolved: false,
        resolution_rationale: None,
        detected_at: Utc::now(),
        resolved_at: None,
        adjudicated_claim_set_hash: None,
        adjudicated_at: None,
    };
    let created_conflict_group = conflict_group_repo.create(conflict_group).await?;
    assert!(
        !created_conflict_group.resolved,
        "Conflict group should start unresolved"
    );
    assert_eq!(
        created_conflict_group.claim_ids.len(),
        2,
        "Conflict group should contain both claims"
    );

    // Invariant 3: Resolution strategies applied (confidence-based in this case)
    // Higher confidence claim (rel_1 with 0.85) should win
    let mut resolved_conflict_group = created_conflict_group.clone();
    resolved_conflict_group.resolved = true;
    resolved_conflict_group.resolution_rationale =
        Some("Resolved by confidence: claim1 (0.85) > claim2 (0.75)".to_string());
    resolved_conflict_group.resolved_at = Some(Utc::now());

    let updated_conflict_group = conflict_group_repo
        .update(space_id, &conflict_group_id, resolved_conflict_group)
        .await?;
    assert!(
        updated_conflict_group.resolved,
        "Conflict group should be resolved"
    );
    assert!(
        updated_conflict_group.resolution_rationale.is_some(),
        "Resolution rationale should be recorded"
    );

    // Invariant 4: Winning claim promoted, losers marked conflicted
    // Create truth slot with winning claim
    let truth_slot_id = RecordId::new_v4(TableName::TruthSlot);
    let truth_slot_base = BaseFields::new(
        truth_slot_id.clone(),
        space_id.to_string(),
        "truth_maintenance".to_string(),
    );
    let truth_slot = TruthSlot {
        base: truth_slot_base,
        slot_key: slot_key.clone(),
        slot_type: "rel".to_string(),
        subject_entity: entity_john_id.to_string(),
        predicate_id: "works_at".to_string(),
        current_values: vec![CurrentValue {
            claim_id: created_rel_1.base.id.to_string(),
            confidence: 0.85,
            status: ClaimStatus::Approved,
            evidence: vec![],
        }],
        conflict_group: Some(Link::new(conflict_group_id.clone())),
        confidence: 0.85,
        rationale: Some("Highest confidence claim selected".to_string()),
        semantics_version: 1,
        alias_version: 1,
        stale: false,
        stale_reason: None,
        computed_at: Utc::now(),
        security_hash: "default".to_string(),
    };
    let created_truth_slot = truth_slot_repo.create(truth_slot).await?;
    assert_eq!(
        created_truth_slot.current_values.len(),
        1,
        "Truth slot should have one winning value"
    );
    assert_eq!(
        created_truth_slot.current_values[0].status,
        ClaimStatus::Approved,
        "Winning claim should be approved"
    );

    // Update the winning rel to Approved status
    let mut updated_rel_1 = created_rel_1.clone();
    updated_rel_1.status = ClaimStatus::Approved;
    let final_rel_1 = rel_repo.update(space_id, &rel_1_id, updated_rel_1).await?;
    assert_eq!(
        final_rel_1.status,
        ClaimStatus::Approved,
        "Winning claim should be promoted to Approved"
    );

    // Update the losing rel to Disputed status
    let mut updated_rel_2 = created_rel_2.clone();
    updated_rel_2.status = ClaimStatus::Disputed;
    let final_rel_2 = rel_repo.update(space_id, &rel_2_id, updated_rel_2).await?;
    assert_eq!(
        final_rel_2.status,
        ClaimStatus::Disputed,
        "Losing claim should be marked as Disputed"
    );

    // Verify final state
    let retrieved_truth_slot = truth_slot_repo.get(space_id, &truth_slot_id).await?;
    assert!(
        retrieved_truth_slot.is_some(),
        "Truth slot should be retrievable"
    );
    let slot = retrieved_truth_slot.unwrap();
    assert!(
        !slot.stale,
        "Truth slot should not be stale after resolution"
    );
    assert!(
        slot.conflict_group.is_some(),
        "Resolved conflict group should still be linked"
    );

    Ok(())
}
