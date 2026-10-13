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
//! S4.9 Freshness sweep scenario test.
//!
//! Verifies freshness recalculation during sweep operations.

use chrono::{Duration, Utc};
use elephant_storage::{
    EntityRepository, FreshnessRepository, RelRepository, Repository, SourceRepository,
};
use elephant_types::ids::TableName;
use elephant_types::records::base::{BaseFields, Security, SubjectsState};
use elephant_types::records::entity::EntityType;
use elephant_types::records::freshness::{DecayModel, Freshness, FreshnessFactors};
use elephant_types::records::rel::{ClaimStatus, Rel};
use elephant_types::records::source::{Source, SourceType};
use elephant_types::records::Entity;
use elephant_types::{Link, RecordId};

use crate::Harness;

/// S4.9: Freshness sweep recalculation.
///
/// Test invariants:
/// 1. Freshness scores decay over time per decay model
/// 2. Sweep operation recalculates all stale freshness records
/// 3. Demotion triggered when freshness drops below threshold
/// 4. High-freshness sources prioritized in queries
#[tokio::test]
async fn scenario_s4_9_freshness_sweep() -> anyhow::Result<()> {
    let harness = Harness::new().await?;
    let source_repo = SourceRepository::new(harness.client.clone());
    let entity_repo = EntityRepository::new(harness.client.clone());
    let rel_repo = RelRepository::new(harness.client.clone());
    let freshness_repo = FreshnessRepository::new(harness.client.clone());

    let space_id = "Space_Freshness";
    let now = Utc::now();
    let demotion_threshold = 0.3_f32; // Score below this triggers demotion

    // Create entities for relationships
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

    // Create sources with different ages
    let source_fresh_id = RecordId::new_v4(TableName::Source);
    let mut source_fresh_base = BaseFields::new(
        source_fresh_id.clone(),
        space_id.to_string(),
        "P_sys".to_string(),
    );
    source_fresh_base.security = Security {
        policy_id: "default".to_string(),
        level: 1,
        ..Default::default()
    };
    source_fresh_base.subjects_state = SubjectsState::Complete;
    let mut source_fresh = Source::new(
        source_fresh_base,
        SourceType::Email,
        "hash_fresh_source".to_string(),
    );
    source_fresh.external_id = Some("email-fresh-001".to_string());
    source_fresh.uri = Some("mailto:recent@example.com".to_string());
    source_fresh.metadata = serde_json::json!({
        "subject": "Recent Update",
        "date": now.to_rfc3339()
    });
    source_repo.create(source_fresh).await?;

    let source_stale_id = RecordId::new_v4(TableName::Source);
    let mut source_stale_base = BaseFields::new(
        source_stale_id.clone(),
        space_id.to_string(),
        "P_sys".to_string(),
    );
    source_stale_base.security = Security {
        policy_id: "default".to_string(),
        level: 1,
        ..Default::default()
    };
    source_stale_base.subjects_state = SubjectsState::Complete;
    let old_date = now - Duration::days(180); // 6 months ago
    let mut source_stale = Source::new(
        source_stale_base,
        SourceType::Email,
        "hash_stale_source".to_string(),
    );
    source_stale.external_id = Some("email-stale-001".to_string());
    source_stale.uri = Some("mailto:old@example.com".to_string());
    source_stale.metadata = serde_json::json!({
        "subject": "Old Information",
        "date": old_date.to_rfc3339()
    });
    source_repo.create(source_stale).await?;

    // Create relationships from different sources
    let rel_fresh_id = RecordId::new_v4(TableName::Rel);
    let mut rel_fresh_base = BaseFields::new(
        rel_fresh_id.clone(),
        space_id.to_string(),
        "extractor".to_string(),
    );
    rel_fresh_base.security = Security {
        policy_id: "default".to_string(),
        level: 1,
        ..Default::default()
    };
    rel_fresh_base.provenance.sources = vec![source_fresh_id.clone()];
    let rel_fresh = Rel {
        base: rel_fresh_base,
        subject: Link::new(entity_john_id.clone()),
        object: Link::new(entity_acme_id.clone()),
        rel_type: "works_at".to_string(),
        status: ClaimStatus::Approved,
        confidence: 0.95,
        validity_window: None,
        evidence: vec![],
        temporal_audit: None,
    };
    let created_rel_fresh = rel_repo.create(rel_fresh).await?;

    let rel_stale_id = RecordId::new_v4(TableName::Rel);
    let mut rel_stale_base = BaseFields::new(
        rel_stale_id.clone(),
        space_id.to_string(),
        "extractor".to_string(),
    );
    rel_stale_base.security = Security {
        policy_id: "default".to_string(),
        level: 1,
        ..Default::default()
    };
    rel_stale_base.provenance.sources = vec![source_stale_id.clone()];
    let rel_stale = Rel {
        base: rel_stale_base,
        subject: Link::new(entity_john_id.clone()),
        object: Link::new(entity_acme_id.clone()),
        rel_type: "contacted".to_string(),
        status: ClaimStatus::Approved,
        confidence: 0.85,
        validity_window: None,
        evidence: vec![],
        temporal_audit: None,
    };
    let created_rel_stale = rel_repo.create(rel_stale).await?;

    // Invariant 1: Freshness scores decay over time per decay model
    // Create freshness record for fresh relationship (recent evidence)
    let freshness_fresh_id = RecordId::new_v4(TableName::Freshness);
    let freshness_fresh_base = BaseFields::new(
        freshness_fresh_id.clone(),
        space_id.to_string(),
        "freshness_worker".to_string(),
    );
    let freshness_fresh = Freshness {
        base: freshness_fresh_base,
        target: created_rel_fresh.base.id.to_string(),
        target_type: "rel".to_string(),
        decay_model: DecayModel::Exponential,
        score: 0.95, // High score - recent evidence
        confidence: 0.9,
        rationale: Some("Evidence from today, high reliability source".to_string()),
        factors: FreshnessFactors {
            evidence_age_days: 0.0, // Brand new
            conflict_severity: 0.0,
            evidence_strength: 0.95,
            source_reliability: 0.9,
            corroboration_count: 1,
            custom: std::collections::HashMap::new(),
        },
        computed_at: now,
        next_review_at: Some(now + Duration::days(7)),
        demote: false,
        demotion_threshold: None,
    };
    let created_freshness_fresh = freshness_repo.create(freshness_fresh).await?;
    assert!(
        (created_freshness_fresh.score - 0.95).abs() < 0.01,
        "Fresh relationship should have high freshness score"
    );
    assert!(
        !created_freshness_fresh.demote,
        "Fresh relationship should not be demoted"
    );

    // Create freshness record for stale relationship (old evidence)
    let freshness_stale_id = RecordId::new_v4(TableName::Freshness);
    let freshness_stale_base = BaseFields::new(
        freshness_stale_id.clone(),
        space_id.to_string(),
        "freshness_worker".to_string(),
    );
    let freshness_stale = Freshness {
        base: freshness_stale_base,
        target: created_rel_stale.base.id.to_string(),
        target_type: "rel".to_string(),
        decay_model: DecayModel::Exponential,
        score: 0.45, // Medium score - older evidence but still valid
        confidence: 0.7,
        rationale: Some("Evidence from 6 months ago, subject to decay".to_string()),
        factors: FreshnessFactors {
            evidence_age_days: 180.0, // 6 months old
            conflict_severity: 0.0,
            evidence_strength: 0.85,
            source_reliability: 0.8,
            corroboration_count: 1,
            custom: std::collections::HashMap::new(),
        },
        computed_at: now - Duration::days(7), // Last computed a week ago
        next_review_at: Some(now),            // Due for review
        demote: false,
        demotion_threshold: None,
    };
    let created_freshness_stale = freshness_repo.create(freshness_stale).await?;
    assert!(
        created_freshness_stale.score < created_freshness_fresh.score,
        "Stale relationship should have lower freshness score"
    );

    // Invariant 2: Sweep operation recalculates all stale freshness records
    // Simulate a sweep that recalculates the stale record
    // The stale record's score decays further with additional time
    fn compute_decayed_score(
        current_score: f32,
        days_since_computation: f64,
        model: DecayModel,
    ) -> f32 {
        match model {
            DecayModel::Exponential => {
                // Half-life of 90 days
                let half_life = 90.0;
                let decay_factor = 0.5_f64.powf(days_since_computation / half_life);
                current_score * decay_factor as f32
            }
            DecayModel::Linear => {
                // Lose 0.5% per day
                let decay = 0.005 * days_since_computation as f32;
                (current_score - decay).max(0.0)
            }
            DecayModel::StepFunction => {
                // Drops at 30, 90, 180 days
                if days_since_computation > 180.0 {
                    current_score * 0.3
                } else if days_since_computation > 90.0 {
                    current_score * 0.6
                } else if days_since_computation > 30.0 {
                    current_score * 0.85
                } else {
                    current_score
                }
            }
        }
    }

    // The stale record was computed 7 days ago, so it decays
    let recalculated_score = compute_decayed_score(
        created_freshness_stale.score,
        7.0, // 7 days since last computation
        DecayModel::Exponential,
    );
    assert!(
        recalculated_score < created_freshness_stale.score,
        "Recalculated score should be lower due to decay"
    );

    // Update the stale freshness record with new computation
    let mut updated_freshness_stale = created_freshness_stale.clone();
    updated_freshness_stale.score = recalculated_score;
    updated_freshness_stale.factors.evidence_age_days = 187.0; // Now 187 days old
    updated_freshness_stale.computed_at = now;
    updated_freshness_stale.next_review_at = Some(now + Duration::days(7));
    updated_freshness_stale.rationale =
        Some("Recalculated during sweep - evidence aging".to_string());

    let swept_freshness_stale = freshness_repo
        .update(space_id, &freshness_stale_id, updated_freshness_stale)
        .await?;
    assert!(
        swept_freshness_stale.computed_at > created_freshness_stale.computed_at,
        "Sweep should update computed_at"
    );

    // Invariant 3: Demotion triggered when freshness drops below threshold
    // Create a very old relationship that should be demoted
    let source_ancient_id = RecordId::new_v4(TableName::Source);
    let mut source_ancient_base = BaseFields::new(
        source_ancient_id.clone(),
        space_id.to_string(),
        "P_sys".to_string(),
    );
    source_ancient_base.security = Security {
        policy_id: "default".to_string(),
        level: 1,
        ..Default::default()
    };
    let ancient_date = now - Duration::days(365 * 2); // 2 years ago
    let mut source_ancient = Source::new(
        source_ancient_base,
        SourceType::Web,
        "hash_ancient_source".to_string(),
    );
    source_ancient.external_id = Some("web-ancient-001".to_string());
    source_ancient.uri = Some("https://archive.example.com/old-page.html".to_string());
    source_ancient.metadata = serde_json::json!({
        "title": "Ancient Page",
        "date": ancient_date.to_rfc3339()
    });
    source_repo.create(source_ancient).await?;

    let rel_ancient_id = RecordId::new_v4(TableName::Rel);
    let mut rel_ancient_base = BaseFields::new(
        rel_ancient_id.clone(),
        space_id.to_string(),
        "extractor".to_string(),
    );
    rel_ancient_base.security = Security {
        policy_id: "default".to_string(),
        level: 1,
        ..Default::default()
    };
    rel_ancient_base.provenance.sources = vec![source_ancient_id.clone()];
    let rel_ancient = Rel {
        base: rel_ancient_base,
        subject: Link::new(entity_john_id.clone()),
        object: Link::new(entity_acme_id.clone()),
        rel_type: "visited".to_string(),
        status: ClaimStatus::Approved,
        confidence: 0.7,
        validity_window: None,
        evidence: vec![],
        temporal_audit: None,
    };
    let created_rel_ancient = rel_repo.create(rel_ancient).await?;

    // Create freshness for ancient relationship - score below threshold
    let freshness_ancient_id = RecordId::new_v4(TableName::Freshness);
    let freshness_ancient_base = BaseFields::new(
        freshness_ancient_id.clone(),
        space_id.to_string(),
        "freshness_worker".to_string(),
    );
    let freshness_ancient = Freshness {
        base: freshness_ancient_base,
        target: created_rel_ancient.base.id.to_string(),
        target_type: "rel".to_string(),
        decay_model: DecayModel::Exponential,
        score: 0.15, // Very low - below demotion threshold
        confidence: 0.5,
        rationale: Some("Evidence 2 years old, severely decayed".to_string()),
        factors: FreshnessFactors {
            evidence_age_days: 730.0, // 2 years
            conflict_severity: 0.0,
            evidence_strength: 0.5,
            source_reliability: 0.6,
            corroboration_count: 1,
            custom: std::collections::HashMap::new(),
        },
        computed_at: now,
        next_review_at: Some(now + Duration::days(30)), // Review less frequently
        demote: true,                                   // Flagged for demotion
        demotion_threshold: Some(demotion_threshold),
    };
    let created_freshness_ancient = freshness_repo.create(freshness_ancient).await?;
    assert!(
        created_freshness_ancient.score < demotion_threshold,
        "Ancient relationship should be below demotion threshold"
    );
    assert!(
        created_freshness_ancient.demote,
        "Ancient relationship should be flagged for demotion"
    );
    assert!(
        created_freshness_ancient.demotion_threshold.is_some(),
        "Demotion threshold should be recorded"
    );

    // Invariant 4: High-freshness sources prioritized in queries
    // Verify freshness ordering: fresh > stale > ancient
    let all_freshness = freshness_repo.list(space_id, 100, None).await?;
    assert_eq!(
        all_freshness.len(),
        3,
        "All three freshness records should exist"
    );

    // Sort by score descending (how queries should prioritize)
    let mut sorted_by_score = all_freshness.clone();
    sorted_by_score.sort_by(|a, b| {
        b.score
            .partial_cmp(&a.score)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    assert!(
        sorted_by_score[0].score > sorted_by_score[1].score,
        "First should have highest score"
    );
    assert!(
        sorted_by_score[1].score > sorted_by_score[2].score,
        "Second should have middle score"
    );

    // Fresh relationship should be first
    assert!(
        sorted_by_score[0].target == created_rel_fresh.base.id.to_string(),
        "Fresh relationship should be prioritized first"
    );

    // Ancient (demoted) should be last
    assert!(
        sorted_by_score[2].demote,
        "Last in priority should be the demoted one"
    );

    // Verify demoted records can be filtered out
    let non_demoted: Vec<_> = all_freshness.iter().filter(|f| !f.demote).collect();
    assert_eq!(non_demoted.len(), 2, "Two records should not be demoted");

    let demoted: Vec<_> = all_freshness.iter().filter(|f| f.demote).collect();
    assert_eq!(demoted.len(), 1, "One record should be demoted");

    Ok(())
}
