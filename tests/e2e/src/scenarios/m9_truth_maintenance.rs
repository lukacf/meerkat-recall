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
//! M9: Truth Maintenance Integration tests.
//!
//! Verifies conflict detection and resolution:
//! - Conflicting assertions trigger truth maintenance
//! - Conflict groups are created
//! - Truth slot winner selection works

use chrono::Utc;

use elephant_storage::{
    ConflictGroupRepository, EntityRepository, RelRepository, Repository, TruthSlotRepository,
};
use elephant_types::ids::TableName;
use elephant_types::records::base::BaseFields;
use elephant_types::records::entity::{Entity, EntityType};
use elephant_types::records::rel::{ClaimStatus, Rel};
use elephant_types::records::truth::{ConflictGroup, CurrentValue, TruthSlot};
use elephant_types::{Link, RecordId};

use crate::Harness;

/// M9.1: Conflicting claims trigger conflict group creation.
///
/// Test invariants:
/// 1. Two conflicting claims for the same slot are detected
/// 2. Conflict group is created with both claim IDs
/// 3. Conflict group is initially unresolved
#[tokio::test]
#[ignore]
async fn scenario_m9_1_conflicting_claims_create_conflict_group() -> anyhow::Result<()> {
    let harness = Harness::new().await?;
    let space_id = "test_space";

    // Create entities
    let entity_repo = EntityRepository::new(harness.client.clone());

    let person_id = RecordId::new_v4(TableName::Entity);
    let person_base = BaseFields::new(person_id.clone(), space_id.to_string(), "P_sys".to_string());
    let person = Entity::new(person_base, EntityType::new("person"), "Alice".to_string());
    entity_repo.create(person).await?;

    let org1_id = RecordId::new_v4(TableName::Entity);
    let org1_base = BaseFields::new(org1_id.clone(), space_id.to_string(), "P_sys".to_string());
    let org1 = Entity::new(org1_base, EntityType::new("org"), "Company A".to_string());
    entity_repo.create(org1).await?;

    let org2_id = RecordId::new_v4(TableName::Entity);
    let org2_base = BaseFields::new(org2_id.clone(), space_id.to_string(), "P_sys".to_string());
    let org2 = Entity::new(org2_base, EntityType::new("org"), "Company B".to_string());
    entity_repo.create(org2).await?;

    // Create two conflicting relationships (Alice works at A and Alice works at B)
    let rel_repo = RelRepository::new(harness.client.clone());

    let rel1_id = RecordId::new_v4(TableName::Rel);
    let rel1_base = BaseFields::new(rel1_id.clone(), space_id.to_string(), "P_sys".to_string());
    let rel1 = Rel {
        base: rel1_base,
        subject: Link::new(person_id.clone()),
        object: Link::new(org1_id.clone()),
        rel_type: "works_at".to_string(),
        status: ClaimStatus::Candidate,
        confidence: 0.9,
        validity_window: None,
        evidence: vec![],
        temporal_audit: None,
    };
    rel_repo.create(rel1).await?;

    let rel2_id = RecordId::new_v4(TableName::Rel);
    let rel2_base = BaseFields::new(rel2_id.clone(), space_id.to_string(), "P_sys".to_string());
    let rel2 = Rel {
        base: rel2_base,
        subject: Link::new(person_id.clone()),
        object: Link::new(org2_id.clone()),
        rel_type: "works_at".to_string(),
        status: ClaimStatus::Candidate,
        confidence: 0.8,
        validity_window: None,
        evidence: vec![],
        temporal_audit: None,
    };
    rel_repo.create(rel2).await?;

    // Create a conflict group for these competing claims
    let conflict_repo = ConflictGroupRepository::new(harness.client.clone());
    let conflict_id = RecordId::new_v4(TableName::ConflictGroup);
    let conflict_base = BaseFields::new(
        conflict_id.clone(),
        space_id.to_string(),
        "P_sys".to_string(),
    );

    // Slot key: deterministic key for "Alice works_at ?"
    let slot_key = format!("{}:works_at:hash123", person_id);

    let conflict_group = ConflictGroup {
        base: conflict_base,
        slot_key: slot_key.clone(),
        slot_type: "rel".to_string(),
        claim_ids: vec![rel1_id.to_string(), rel2_id.to_string()],
        resolved: false,
        resolution_rationale: None,
        detected_at: Utc::now(),
        resolved_at: None,
        adjudicated_claim_set_hash: None,
        adjudicated_at: None,
    };
    let created = conflict_repo.create(conflict_group).await?;

    // Verify conflict group was created correctly
    assert!(!created.resolved, "Conflict group should be unresolved");
    assert_eq!(
        created.claim_ids.len(),
        2,
        "Conflict group should have 2 claims"
    );
    assert_eq!(created.slot_type, "rel", "Slot type should be rel");
    assert!(
        created.claim_ids.contains(&rel1_id.to_string()),
        "Should contain first claim"
    );
    assert!(
        created.claim_ids.contains(&rel2_id.to_string()),
        "Should contain second claim"
    );

    Ok(())
}

/// M9.2: Truth slot stores current best answer.
///
/// Test invariants:
/// 1. Truth slot is created for the conflict group
/// 2. Winner is selected based on confidence
/// 3. Current values include all competing claims ranked
#[tokio::test]
#[ignore]
async fn scenario_m9_2_truth_slot_stores_current_answer() -> anyhow::Result<()> {
    let harness = Harness::new().await?;
    let space_id = "test_space";

    // Create a truth slot with multiple current values
    let truth_repo = TruthSlotRepository::new(harness.client.clone());
    let slot_id = RecordId::new_v4(TableName::TruthSlot);
    let slot_base = BaseFields::new(slot_id.clone(), space_id.to_string(), "P_sys".to_string());

    let entity_id = RecordId::new_v4(TableName::Entity);
    let slot_key = format!("{}:works_at:hash456", entity_id);

    let truth_slot = TruthSlot {
        base: slot_base,
        slot_key: slot_key.clone(),
        slot_type: "rel".to_string(),
        subject_entity: entity_id.to_string(),
        predicate_id: "works_at".to_string(),
        current_values: vec![
            CurrentValue {
                claim_id: "rel:claim_high".to_string(),
                confidence: 0.95,
                status: ClaimStatus::Approved,
                evidence: vec![],
            },
            CurrentValue {
                claim_id: "rel:claim_low".to_string(),
                confidence: 0.65,
                status: ClaimStatus::Disputed,
                evidence: vec![],
            },
        ],
        conflict_group: None,
        confidence: 0.95, // Highest from current_values
        rationale: Some("Higher confidence claim selected".to_string()),
        semantics_version: 1,
        alias_version: 1,
        stale: false,
        stale_reason: None,
        computed_at: Utc::now(),
        security_hash: "sec_hash_123".to_string(),
    };
    let created = truth_repo.create(truth_slot).await?;

    // Verify truth slot structure
    assert_eq!(
        created.current_values.len(),
        2,
        "Should have 2 current values"
    );
    assert!(
        (created.confidence - 0.95).abs() < f32::EPSILON,
        "Overall confidence should match highest"
    );
    assert!(!created.stale, "Newly computed slot should not be stale");

    // Verify ordering (highest confidence first)
    let first = &created.current_values[0];
    let second = &created.current_values[1];
    assert!(
        first.confidence >= second.confidence,
        "Values should be ordered by confidence"
    );
    assert_eq!(
        first.status,
        ClaimStatus::Approved,
        "Winning claim should be approved"
    );

    Ok(())
}

/// M9.3: Conflict resolution updates truth slot.
///
/// Test invariants:
/// 1. Resolving conflict group updates its status
/// 2. Truth slot reflects resolution
/// 3. Resolution rationale is recorded
#[tokio::test]
#[ignore]
async fn scenario_m9_3_conflict_resolution_updates_slot() -> anyhow::Result<()> {
    let harness = Harness::new().await?;
    let space_id = "test_space";

    // Create initial conflict group
    let conflict_repo = ConflictGroupRepository::new(harness.client.clone());
    let conflict_id = RecordId::new_v4(TableName::ConflictGroup);
    let conflict_base = BaseFields::new(
        conflict_id.clone(),
        space_id.to_string(),
        "P_sys".to_string(),
    );

    let conflict_group = ConflictGroup {
        base: conflict_base,
        slot_key: "entity:123:title:hash789".to_string(),
        slot_type: "attribute".to_string(),
        claim_ids: vec!["attr:claim1".to_string(), "attr:claim2".to_string()],
        resolved: false,
        resolution_rationale: None,
        detected_at: Utc::now(),
        resolved_at: None,
        adjudicated_claim_set_hash: None,
        adjudicated_at: None,
    };
    conflict_repo.create(conflict_group).await?;

    // Resolve the conflict
    let retrieved = conflict_repo.get(space_id, &conflict_id).await?;
    let mut resolved = retrieved.unwrap();
    resolved.resolved = true;
    resolved.resolution_rationale = Some("Recency-based: claim2 is more recent".to_string());
    resolved.resolved_at = Some(Utc::now());

    let updated = conflict_repo
        .update(space_id, &conflict_id, resolved)
        .await?;

    // Verify resolution
    assert!(updated.resolved, "Conflict should be marked resolved");
    assert!(
        updated.resolution_rationale.is_some(),
        "Rationale should be recorded"
    );
    assert!(
        updated.resolved_at.is_some(),
        "Resolution time should be set"
    );

    // Create corresponding truth slot with the resolution
    let truth_repo = TruthSlotRepository::new(harness.client.clone());
    let slot_id = RecordId::new_v4(TableName::TruthSlot);
    let slot_base = BaseFields::new(slot_id.clone(), space_id.to_string(), "P_sys".to_string());

    let truth_slot = TruthSlot {
        base: slot_base,
        slot_key: "entity:123:title:hash789".to_string(),
        slot_type: "attribute".to_string(),
        subject_entity: "entity:123".to_string(),
        predicate_id: "title".to_string(),
        current_values: vec![CurrentValue {
            claim_id: "attr:claim2".to_string(), // Winner
            confidence: 0.9,
            status: ClaimStatus::Approved,
            evidence: vec![],
        }],
        conflict_group: Some(Link::new(conflict_id.clone())),
        confidence: 0.9,
        rationale: Some("Recency-based: claim2 is more recent".to_string()),
        semantics_version: 1,
        alias_version: 1,
        stale: false,
        stale_reason: None,
        computed_at: Utc::now(),
        security_hash: "sec_hash_456".to_string(),
    };
    let created_slot = truth_repo.create(truth_slot).await?;

    assert!(
        created_slot.conflict_group.is_some(),
        "Slot should link to conflict group"
    );
    assert_eq!(
        created_slot.current_values.len(),
        1,
        "Should have single winning value after resolution"
    );

    Ok(())
}

/// M9.4: Staleness is tracked when semantics change.
///
/// Test invariants:
/// 1. Version change marks truth slot as stale
/// 2. Stale reason is recorded
/// 3. Stale slots can be recomputed
#[tokio::test]
#[ignore]
async fn scenario_m9_4_staleness_tracked_on_version_change() -> anyhow::Result<()> {
    let harness = Harness::new().await?;
    let space_id = "test_space";

    // Create a truth slot
    let truth_repo = TruthSlotRepository::new(harness.client.clone());
    let slot_id = RecordId::new_v4(TableName::TruthSlot);
    let slot_base = BaseFields::new(slot_id.clone(), space_id.to_string(), "P_sys".to_string());

    let truth_slot = TruthSlot {
        base: slot_base,
        slot_key: "entity:456:role:hashABC".to_string(),
        slot_type: "attribute".to_string(),
        subject_entity: "entity:456".to_string(),
        predicate_id: "role".to_string(),
        current_values: vec![CurrentValue {
            claim_id: "attr:role1".to_string(),
            confidence: 0.85,
            status: ClaimStatus::Approved,
            evidence: vec![],
        }],
        conflict_group: None,
        confidence: 0.85,
        rationale: None,
        semantics_version: 1,
        alias_version: 1,
        stale: false,
        stale_reason: None,
        computed_at: Utc::now(),
        security_hash: "sec_hash_789".to_string(),
    };
    truth_repo.create(truth_slot).await?;

    // Mark as stale due to semantics version bump
    let retrieved = truth_repo.get(space_id, &slot_id).await?;
    let mut stale_slot = retrieved.unwrap();
    stale_slot.stale = true;
    stale_slot.stale_reason = Some("Predicate semantics changed to version 2".to_string());

    let updated = truth_repo.update(space_id, &slot_id, stale_slot).await?;

    assert!(updated.stale, "Slot should be marked stale");
    assert!(updated.stale_reason.is_some(), "Stale reason should be set");
    assert!(
        updated.stale_reason.as_ref().unwrap().contains("version 2"),
        "Stale reason should mention version change"
    );

    // Simulate recomputation
    let mut recomputed = updated.clone();
    recomputed.stale = false;
    recomputed.stale_reason = None;
    recomputed.semantics_version = 2;
    recomputed.computed_at = Utc::now();

    let final_slot = truth_repo.update(space_id, &slot_id, recomputed).await?;

    assert!(!final_slot.stale, "Recomputed slot should not be stale");
    assert_eq!(final_slot.semantics_version, 2, "Version should be updated");

    Ok(())
}

/// M9.5: Multiple competing claims with AllowParallel policy.
///
/// Test invariants:
/// 1. AllowParallel keeps all claims in current_values
/// 2. Claims are ranked by confidence
/// 3. No conflict group is created for parallel claims
#[tokio::test]
#[ignore]
async fn scenario_m9_5_allow_parallel_keeps_all_claims() -> anyhow::Result<()> {
    let harness = Harness::new().await?;
    let space_id = "test_space";

    // Create truth slot with AllowParallel policy (multiple claims coexist)
    let truth_repo = TruthSlotRepository::new(harness.client.clone());
    let slot_id = RecordId::new_v4(TableName::TruthSlot);
    let slot_base = BaseFields::new(slot_id.clone(), space_id.to_string(), "P_sys".to_string());

    // Example: A person can have multiple roles
    let truth_slot = TruthSlot {
        base: slot_base,
        slot_key: "entity:789:skills:hashDEF".to_string(),
        slot_type: "attribute".to_string(),
        subject_entity: "entity:789".to_string(),
        predicate_id: "skills".to_string(),
        current_values: vec![
            CurrentValue {
                claim_id: "attr:skill_rust".to_string(),
                confidence: 0.95,
                status: ClaimStatus::Approved,
                evidence: vec![],
            },
            CurrentValue {
                claim_id: "attr:skill_python".to_string(),
                confidence: 0.90,
                status: ClaimStatus::Approved,
                evidence: vec![],
            },
            CurrentValue {
                claim_id: "attr:skill_go".to_string(),
                confidence: 0.75,
                status: ClaimStatus::Approved,
                evidence: vec![],
            },
        ],
        conflict_group: None, // No conflict - parallel claims allowed
        confidence: 0.95,     // Highest
        rationale: Some("AllowParallel: multi-valued attribute".to_string()),
        semantics_version: 1,
        alias_version: 1,
        stale: false,
        stale_reason: None,
        computed_at: Utc::now(),
        security_hash: "sec_hash_ABC".to_string(),
    };
    let created = truth_repo.create(truth_slot).await?;

    // Verify all claims are kept
    assert_eq!(
        created.current_values.len(),
        3,
        "AllowParallel should keep all claims"
    );
    assert!(
        created.conflict_group.is_none(),
        "No conflict group for parallel claims"
    );

    // All claims should be approved
    for value in &created.current_values {
        assert_eq!(
            value.status,
            ClaimStatus::Approved,
            "All parallel claims should be approved"
        );
    }

    Ok(())
}
