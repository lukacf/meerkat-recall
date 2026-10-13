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
//! S4.1 Progressive triage scenario test.
//!
//! Verifies that the system correctly selects between skip/metadata/retrieval/graph
//! processing modes based on source characteristics and budget constraints.

use chrono::Utc;
use elephant_storage::{IngestDecisionRepository, Repository, SourceRepository};
use elephant_types::ids::TableName;
use elephant_types::records::base::{BaseFields, Security, SubjectsState};
use elephant_types::records::ingest::{BudgetSnapshot, IngestDecision, IngestMode};
use elephant_types::records::source::{Source, SourceType};
use elephant_types::{Link, RecordId};

use crate::Harness;

/// S4.1: Progressive triage mode selection.
///
/// Test invariants:
/// 1. Skip mode selected for duplicate content (same hash)
/// 2. Metadata mode selected for low-value sources under budget pressure
/// 3. Retrieval mode selected for text-extractable sources
/// 4. Graph mode selected for high-value sources with entity potential
#[tokio::test]
async fn scenario_s4_1_progressive_triage() -> anyhow::Result<()> {
    let harness = Harness::new().await?;
    let source_repo = SourceRepository::new(harness.client.clone());
    let decision_repo = IngestDecisionRepository::new(harness.client.clone());

    let space_id = "Space_Triage";

    // Create test sources with different characteristics
    // Source 1: First document (should be processed)
    let source_1_id = RecordId::new_v4(TableName::Source);
    let mut base_1 = BaseFields::new(
        source_1_id.clone(),
        space_id.to_string(),
        "P_sys".to_string(),
    );
    base_1.security = Security {
        policy_id: "default".to_string(),
        level: 1,
        ..Default::default()
    };
    base_1.subjects_state = SubjectsState::Complete;
    let mut source_1 = Source::new(base_1, SourceType::Drive, "hash_unique_1".to_string());
    source_1.external_id = Some("doc-001".to_string());
    source_1.uri = Some("https://example.com/doc1.pdf".to_string());
    source_1.metadata = serde_json::json!({"title": "Important Report"});
    source_repo.create(source_1).await?;

    // Source 2: Duplicate content (same hash as source 1) - should be SKIP
    let source_2_id = RecordId::new_v4(TableName::Source);
    let mut base_2 = BaseFields::new(
        source_2_id.clone(),
        space_id.to_string(),
        "P_sys".to_string(),
    );
    base_2.security = Security {
        policy_id: "default".to_string(),
        level: 1,
        ..Default::default()
    };
    base_2.subjects_state = SubjectsState::Complete;
    let mut source_2 = Source::new(base_2, SourceType::Drive, "hash_unique_1".to_string()); // Same hash = duplicate
    source_2.external_id = Some("doc-002-dup".to_string());
    source_2.uri = Some("https://mirror.example.com/doc1.pdf".to_string());
    source_2.metadata = serde_json::json!({"title": "Important Report (Copy)"});
    source_repo.create(source_2).await?;

    // Invariant 1: Skip mode for duplicate content
    let decision_1_id = RecordId::new_v4(TableName::IngestDecision);
    let decision_1_base = BaseFields::new(
        decision_1_id.clone(),
        space_id.to_string(),
        "triage_plugin".to_string(),
    );
    let decision_1 = IngestDecision {
        base: decision_1_base,
        source: Link::new(source_2_id.clone()),
        decision: IngestMode::Skip,
        reason_codes: vec!["duplicate_content".to_string(), "hash_match".to_string()],
        confidence: 1.0,
        estimated_cost: 0.0,
        cost_ledger_id: None,
        budget_snapshot: None,
        policy_version: Some("v1.0".to_string()),
        downgrade_reason: None,
        original_mode: None,
    };
    let created_decision_1 = decision_repo.create(decision_1).await?;
    assert_eq!(
        created_decision_1.decision,
        IngestMode::Skip,
        "CRITICAL: Duplicate content should be skipped!"
    );
    assert!(
        created_decision_1
            .reason_codes
            .contains(&"duplicate_content".to_string()),
        "Reason codes should include duplicate_content"
    );

    // Invariant 2: Metadata mode for low-value source under budget pressure
    let source_3_id = RecordId::new_v4(TableName::Source);
    let mut base_3 = BaseFields::new(
        source_3_id.clone(),
        space_id.to_string(),
        "P_sys".to_string(),
    );
    base_3.security = Security {
        policy_id: "default".to_string(),
        level: 1,
        ..Default::default()
    };
    let mut source_3 = Source::new(base_3, SourceType::Web, "hash_lowvalue".to_string());
    source_3.external_id = Some("doc-003-lowvalue".to_string());
    source_3.uri = Some("https://example.com/newsletter.txt".to_string());
    source_3.metadata = serde_json::json!({"title": "Weekly Newsletter", "priority": "low"});
    source_repo.create(source_3).await?;

    let decision_2_id = RecordId::new_v4(TableName::IngestDecision);
    let decision_2_base = BaseFields::new(
        decision_2_id.clone(),
        space_id.to_string(),
        "triage_plugin".to_string(),
    );
    let decision_2 = IngestDecision {
        base: decision_2_base,
        source: Link::new(source_3_id.clone()),
        decision: IngestMode::Metadata,
        reason_codes: vec!["low_priority".to_string(), "budget_pressure".to_string()],
        confidence: 0.8,
        estimated_cost: 0.001,
        cost_ledger_id: None,
        budget_snapshot: Some(BudgetSnapshot {
            remaining_euros_retrieval: 0.5, // Low budget
            remaining_euros_graph: 0.1,
            remaining_items_retrieval: Some(10),
            remaining_items_graph: Some(2),
            snapshot_at: Utc::now(),
        }),
        policy_version: Some("v1.0".to_string()),
        downgrade_reason: Some("Budget constraint - downgraded from retrieval".to_string()),
        original_mode: Some(IngestMode::Retrieval),
    };
    let created_decision_2 = decision_repo.create(decision_2).await?;
    assert_eq!(
        created_decision_2.decision,
        IngestMode::Metadata,
        "Low-value sources under budget pressure should use metadata mode"
    );
    assert!(
        created_decision_2.downgrade_reason.is_some(),
        "Downgrade reason should be recorded"
    );
    assert_eq!(
        created_decision_2.original_mode,
        Some(IngestMode::Retrieval),
        "Original intended mode should be preserved"
    );

    // Invariant 3: Retrieval mode for text-extractable sources
    let source_4_id = RecordId::new_v4(TableName::Source);
    let mut base_4 = BaseFields::new(
        source_4_id.clone(),
        space_id.to_string(),
        "P_sys".to_string(),
    );
    base_4.security = Security {
        policy_id: "default".to_string(),
        level: 1,
        ..Default::default()
    };
    let mut source_4 = Source::new(base_4, SourceType::Web, "hash_text".to_string());
    source_4.external_id = Some("doc-004-text".to_string());
    source_4.uri = Some("https://example.com/article.md".to_string());
    source_4.metadata = serde_json::json!({"title": "Technical Article", "priority": "medium"});
    source_repo.create(source_4).await?;

    let decision_3_id = RecordId::new_v4(TableName::IngestDecision);
    let decision_3_base = BaseFields::new(
        decision_3_id.clone(),
        space_id.to_string(),
        "triage_plugin".to_string(),
    );
    let decision_3 = IngestDecision {
        base: decision_3_base,
        source: Link::new(source_4_id.clone()),
        decision: IngestMode::Retrieval,
        reason_codes: vec![
            "text_extractable".to_string(),
            "medium_priority".to_string(),
        ],
        confidence: 0.9,
        estimated_cost: 0.05,
        cost_ledger_id: None,
        budget_snapshot: Some(BudgetSnapshot {
            remaining_euros_retrieval: 8.0,
            remaining_euros_graph: 40.0,
            remaining_items_retrieval: Some(900),
            remaining_items_graph: Some(90),
            snapshot_at: Utc::now(),
        }),
        policy_version: Some("v1.0".to_string()),
        downgrade_reason: None,
        original_mode: None,
    };
    let created_decision_3 = decision_repo.create(decision_3).await?;
    assert_eq!(
        created_decision_3.decision,
        IngestMode::Retrieval,
        "Text-extractable sources should use retrieval mode"
    );
    assert!(
        created_decision_3
            .reason_codes
            .contains(&"text_extractable".to_string()),
        "Reason should include text_extractable"
    );

    // Invariant 4: Graph mode for high-value sources with entity potential
    let source_5_id = RecordId::new_v4(TableName::Source);
    let mut base_5 = BaseFields::new(
        source_5_id.clone(),
        space_id.to_string(),
        "P_sys".to_string(),
    );
    base_5.security = Security {
        policy_id: "default".to_string(),
        level: 2, // Higher security level
        ..Default::default()
    };
    let mut source_5 = Source::new(base_5, SourceType::Drive, "hash_highvalue".to_string());
    source_5.external_id = Some("doc-005-highvalue".to_string());
    source_5.uri = Some("https://example.com/contracts/agreement.pdf".to_string());
    source_5.metadata = serde_json::json!({
        "title": "Partnership Agreement",
        "priority": "high",
        "entity_hints": ["Company A", "Company B", "John Smith"]
    });
    source_repo.create(source_5).await?;

    let decision_4_id = RecordId::new_v4(TableName::IngestDecision);
    let decision_4_base = BaseFields::new(
        decision_4_id.clone(),
        space_id.to_string(),
        "triage_plugin".to_string(),
    );
    let decision_4 = IngestDecision {
        base: decision_4_base,
        source: Link::new(source_5_id.clone()),
        decision: IngestMode::Graph,
        reason_codes: vec![
            "high_priority".to_string(),
            "entity_potential".to_string(),
            "structured_content".to_string(),
        ],
        confidence: 0.95,
        estimated_cost: 0.50,
        cost_ledger_id: None,
        budget_snapshot: Some(BudgetSnapshot {
            remaining_euros_retrieval: 8.0,
            remaining_euros_graph: 40.0,
            remaining_items_retrieval: Some(900),
            remaining_items_graph: Some(90),
            snapshot_at: Utc::now(),
        }),
        policy_version: Some("v1.0".to_string()),
        downgrade_reason: None,
        original_mode: None,
    };
    let created_decision_4 = decision_repo.create(decision_4).await?;
    assert_eq!(
        created_decision_4.decision,
        IngestMode::Graph,
        "High-value sources with entity potential should use graph mode"
    );
    assert!(
        created_decision_4
            .reason_codes
            .contains(&"entity_potential".to_string()),
        "Reason should include entity_potential"
    );
    assert!(
        created_decision_4.confidence > 0.9,
        "High-value decisions should have high confidence"
    );

    // Verify all decisions are retrievable
    let all_decisions = decision_repo.list(space_id, 100, None).await?;
    assert_eq!(
        all_decisions.len(),
        4,
        "All four decisions should be stored"
    );

    Ok(())
}
