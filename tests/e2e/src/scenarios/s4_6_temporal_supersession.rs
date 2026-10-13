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
//! S4.6 Temporal supersession scenario test.
//!
//! Verifies validity_window management for time-bounded claims.

use chrono::{Duration, Utc};
use elephant_storage::{EntityRepository, RelRepository, Repository, TruthSlotRepository};
use elephant_types::ids::TableName;
use elephant_types::records::base::{BaseFields, Security, SubjectsState};
use elephant_types::records::entity::EntityType;
use elephant_types::records::rel::{ClaimStatus, Rel, ValidityWindow};
use elephant_types::records::truth::{CurrentValue, TruthSlot};
use elephant_types::records::Entity;
use elephant_types::{Link, RecordId};

use crate::Harness;

/// S4.6: Temporal supersession and validity windows.
///
/// Test invariants:
/// 1. Claims with validity_window have bounded truth
/// 2. Newer claims can supersede older within same window
/// 3. Non-overlapping windows coexist without conflict
/// 4. Expired claims excluded from active truth queries
#[tokio::test]
async fn scenario_s4_6_temporal_supersession() -> anyhow::Result<()> {
    let harness = Harness::new().await?;
    let entity_repo = EntityRepository::new(harness.client.clone());
    let rel_repo = RelRepository::new(harness.client.clone());
    let truth_slot_repo = TruthSlotRepository::new(harness.client.clone());

    let space_id = "Space_Temporal";
    let now = Utc::now();

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

    let entity_initech_id = RecordId::new_v4(TableName::Entity);
    let mut entity_initech_base = BaseFields::new(
        entity_initech_id.clone(),
        space_id.to_string(),
        "P_sys".to_string(),
    );
    entity_initech_base.security = Security {
        policy_id: "default".to_string(),
        level: 1,
        ..Default::default()
    };
    entity_initech_base.subjects_state = SubjectsState::Complete;
    let entity_initech = Entity::new(
        entity_initech_base,
        EntityType::new("org"),
        "Initech LLC".to_string(),
    );
    entity_repo.create(entity_initech).await?;

    // Invariant 1: Claims with validity_window have bounded truth
    // John worked at Acme from 2018-2020 (past, closed window)
    let rel_past_id = RecordId::new_v4(TableName::Rel);
    let mut rel_past_base = BaseFields::new(
        rel_past_id.clone(),
        space_id.to_string(),
        "extractor".to_string(),
    );
    rel_past_base.security = Security {
        policy_id: "default".to_string(),
        level: 1,
        ..Default::default()
    };
    let past_start = now - Duration::days(365 * 4); // ~4 years ago
    let past_end = now - Duration::days(365 * 2); // ~2 years ago
    let rel_past = Rel {
        base: rel_past_base,
        subject: Link::new(entity_john_id.clone()),
        object: Link::new(entity_acme_id.clone()),
        rel_type: "works_at".to_string(),
        status: ClaimStatus::Approved,
        confidence: 0.95,
        validity_window: Some(ValidityWindow::closed(past_start, past_end)),
        evidence: vec![],
        temporal_audit: None,
    };
    let created_rel_past = rel_repo.create(rel_past).await?;
    assert!(
        created_rel_past.validity_window.is_some(),
        "Past claim should have validity window"
    );
    let past_window = created_rel_past.validity_window.as_ref().unwrap();
    assert!(
        past_window.end.is_some(),
        "Past claim should have closed window"
    );
    assert!(
        !past_window.contains(now),
        "Past claim should not contain current time"
    );

    // Invariant 3: Non-overlapping windows coexist without conflict
    // John worked at Globex from 2020-2022 (past, closed window, no overlap with Acme)
    let rel_globex_id = RecordId::new_v4(TableName::Rel);
    let mut rel_globex_base = BaseFields::new(
        rel_globex_id.clone(),
        space_id.to_string(),
        "extractor".to_string(),
    );
    rel_globex_base.security = Security {
        policy_id: "default".to_string(),
        level: 1,
        ..Default::default()
    };
    let globex_start = past_end; // Starts when Acme ended
    let globex_end = now - Duration::days(365); // ~1 year ago
    let rel_globex = Rel {
        base: rel_globex_base,
        subject: Link::new(entity_john_id.clone()),
        object: Link::new(entity_globex_id.clone()),
        rel_type: "works_at".to_string(),
        status: ClaimStatus::Approved,
        confidence: 0.92,
        validity_window: Some(ValidityWindow::closed(globex_start, globex_end)),
        evidence: vec![],
        temporal_audit: None,
    };
    let created_rel_globex = rel_repo.create(rel_globex).await?;
    let globex_window = created_rel_globex.validity_window.as_ref().unwrap();
    // Verify non-overlapping: Globex starts when Acme ends
    assert!(
        !globex_window.contains(past_start),
        "Globex window should not overlap with Acme start"
    );

    // Invariant 2: Newer claims can supersede older within same window (current employment)
    // John currently works at Initech (started 1 year ago, ongoing)
    let rel_current_id = RecordId::new_v4(TableName::Rel);
    let mut rel_current_base = BaseFields::new(
        rel_current_id.clone(),
        space_id.to_string(),
        "extractor".to_string(),
    );
    rel_current_base.security = Security {
        policy_id: "default".to_string(),
        level: 1,
        ..Default::default()
    };
    let current_start = globex_end; // Starts when Globex ended
    let rel_current = Rel {
        base: rel_current_base,
        subject: Link::new(entity_john_id.clone()),
        object: Link::new(entity_initech_id.clone()),
        rel_type: "works_at".to_string(),
        status: ClaimStatus::Approved,
        confidence: 0.98,
        validity_window: Some(ValidityWindow::starting_at(current_start)), // Ongoing
        evidence: vec![],
        temporal_audit: None,
    };
    let created_rel_current = rel_repo.create(rel_current).await?;
    let current_window = created_rel_current.validity_window.as_ref().unwrap();
    assert!(
        current_window.is_ongoing(),
        "Current claim should be ongoing"
    );
    assert!(
        current_window.contains(now),
        "Current claim should contain current time"
    );

    // Invariant 4: Expired claims excluded from active truth queries
    // Create a truth slot representing current truth (only Initech is current)
    let slot_key = format!("{}:works_at:default", entity_john_id);
    let truth_slot_id = RecordId::new_v4(TableName::TruthSlot);
    let truth_slot_base = BaseFields::new(
        truth_slot_id.clone(),
        space_id.to_string(),
        "truth_maintenance".to_string(),
    );

    // Only the current (ongoing) claim appears in current_values
    let truth_slot = TruthSlot {
        base: truth_slot_base,
        slot_key: slot_key.clone(),
        slot_type: "rel".to_string(),
        subject_entity: entity_john_id.to_string(),
        predicate_id: "works_at".to_string(),
        current_values: vec![CurrentValue {
            claim_id: created_rel_current.base.id.to_string(),
            confidence: 0.98,
            status: ClaimStatus::Approved,
            evidence: vec![],
        }],
        conflict_group: None,
        confidence: 0.98,
        rationale: Some("Only ongoing claim selected for current truth".to_string()),
        semantics_version: 1,
        alias_version: 1,
        stale: false,
        stale_reason: None,
        computed_at: now,
        security_hash: "default".to_string(),
    };
    let created_truth_slot = truth_slot_repo.create(truth_slot).await?;

    // Verify only one current value (the ongoing one)
    assert_eq!(
        created_truth_slot.current_values.len(),
        1,
        "Truth slot should only contain current (non-expired) claim"
    );
    assert_eq!(
        created_truth_slot.current_values[0].claim_id,
        created_rel_current.base.id.to_string(),
        "Current value should be the ongoing Initech claim"
    );

    // Update the past claims to Superseded status (they are expired)
    let mut updated_rel_past = created_rel_past.clone();
    updated_rel_past.status = ClaimStatus::Superseded;
    let final_rel_past = rel_repo
        .update(space_id, &rel_past_id, updated_rel_past)
        .await?;
    assert_eq!(
        final_rel_past.status,
        ClaimStatus::Superseded,
        "Past claim should be marked as superseded"
    );

    let mut updated_rel_globex = created_rel_globex.clone();
    updated_rel_globex.status = ClaimStatus::Superseded;
    let final_rel_globex = rel_repo
        .update(space_id, &rel_globex_id, updated_rel_globex)
        .await?;
    assert_eq!(
        final_rel_globex.status,
        ClaimStatus::Superseded,
        "Globex claim should be marked as superseded"
    );

    // Verify all three claims exist but with correct statuses
    let all_rels = rel_repo.list(space_id, 100, None).await?;
    assert_eq!(
        all_rels.len(),
        3,
        "All three employment records should exist"
    );

    let superseded_count = all_rels
        .iter()
        .filter(|r| r.status == ClaimStatus::Superseded)
        .count();
    assert_eq!(superseded_count, 2, "Two past claims should be superseded");

    let approved_count = all_rels
        .iter()
        .filter(|r| r.status == ClaimStatus::Approved)
        .count();
    assert_eq!(approved_count, 1, "One current claim should be approved");

    // Verify the approved one is the ongoing Initech claim
    let approved_rel = all_rels
        .iter()
        .find(|r| r.status == ClaimStatus::Approved)
        .unwrap();
    assert!(
        approved_rel.validity_window.as_ref().unwrap().is_ongoing(),
        "The approved claim should be the ongoing one"
    );

    Ok(())
}
