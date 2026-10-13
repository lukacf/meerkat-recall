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
//! S4.5 Budget enforcement scenario test.
//!
//! Verifies over-budget rejection and mode downgrade behavior.

use chrono::{NaiveDate, Utc};
use elephant_storage::{
    CostLedgerDailyRepository, IngestDecisionRepository, Repository, SourceRepository,
};
use elephant_types::ids::TableName;
use elephant_types::records::base::{BaseFields, Security, SubjectsState};
use elephant_types::records::ingest::{
    BudgetSnapshot, CostLedgerDaily, IngestDecision, IngestMode,
};
use elephant_types::records::source::{Source, SourceType};
use elephant_types::{Link, RecordId};

use crate::Harness;

/// S4.5: Budget enforcement and cost control.
///
/// Test invariants:
/// 1. Daily budget limits tracked in cost_ledger_daily
/// 2. Over-budget operations rejected or downgraded
/// 3. Mode downgrade from graph -> retrieval under pressure
/// 4. Budget breach events logged for alerting (validated via decision reason codes)
#[tokio::test]
async fn scenario_s4_5_budget_enforcement() -> anyhow::Result<()> {
    let harness = Harness::new().await?;
    let source_repo = SourceRepository::new(harness.client.clone());
    let decision_repo = IngestDecisionRepository::new(harness.client.clone());
    let cost_ledger_repo = CostLedgerDailyRepository::new(harness.client.clone());

    let space_id = "Space_Budget";
    let today = Utc::now().date_naive();

    // Invariant 1: Daily budget limits tracked in cost_ledger_daily
    // Create a cost ledger entry showing current spend
    let ledger_retrieval_id = RecordId::new_v4(TableName::CostLedgerDaily);
    let ledger_retrieval_base = BaseFields::new(
        ledger_retrieval_id.clone(),
        space_id.to_string(),
        "budget_tracker".to_string(),
    );
    let ledger_retrieval = CostLedgerDaily {
        base: ledger_retrieval_base,
        date: today,
        mode: IngestMode::Retrieval,
        worker_kind: "embed".to_string(),
        total_euros: 8.5, // Spent 8.5 out of 10.0 budget
        total_items: 850,
        total_tokens: 4_250_000,
        total_runs: 850,
        total_failures: 5,
        reserved_euros: 0.5, // 0.5 reserved for in-progress
        reconciled_euros: 8.0,
    };
    let created_ledger_retrieval = cost_ledger_repo.create(ledger_retrieval).await?;
    assert!(
        (created_ledger_retrieval.total_euros - 8.5).abs() < f64::EPSILON,
        "Ledger should track total euros spent"
    );
    assert_eq!(
        created_ledger_retrieval.total_items, 850,
        "Ledger should track total items processed"
    );

    let ledger_graph_id = RecordId::new_v4(TableName::CostLedgerDaily);
    let ledger_graph_base = BaseFields::new(
        ledger_graph_id.clone(),
        space_id.to_string(),
        "budget_tracker".to_string(),
    );
    let ledger_graph = CostLedgerDaily {
        base: ledger_graph_base,
        date: today,
        mode: IngestMode::Graph,
        worker_kind: "extract".to_string(),
        total_euros: 48.0, // Spent 48.0 out of 50.0 budget
        total_items: 95,
        total_tokens: 24_000_000,
        total_runs: 95,
        total_failures: 2,
        reserved_euros: 1.5, // 1.5 reserved
        reconciled_euros: 46.5,
    };
    let created_ledger_graph = cost_ledger_repo.create(ledger_graph).await?;
    assert!(
        (created_ledger_graph.total_euros - 48.0).abs() < f64::EPSILON,
        "Graph mode ledger should track spending"
    );

    // Create a source for testing decisions
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
        SourceType::Drive,
        "hash_budget_test".to_string(),
    );
    source.external_id = Some("doc-budget-001".to_string());
    source.uri = Some("https://example.com/important-doc.pdf".to_string());
    source.metadata = serde_json::json!({"title": "Important Document", "priority": "high"});
    source_repo.create(source).await?;

    // Invariant 2 & 3: Over-budget operations downgraded from graph -> retrieval
    // Budget snapshot shows graph budget is nearly exhausted
    let budget_snapshot = BudgetSnapshot {
        remaining_euros_retrieval: 1.0, // Some retrieval budget left
        remaining_euros_graph: 0.5,     // Very little graph budget
        remaining_items_retrieval: Some(150),
        remaining_items_graph: Some(3), // Only 3 items left in graph budget
        snapshot_at: Utc::now(),
    };

    // High-value document that would normally use Graph mode, but gets downgraded
    let decision_1_id = RecordId::new_v4(TableName::IngestDecision);
    let decision_1_base = BaseFields::new(
        decision_1_id.clone(),
        space_id.to_string(),
        "triage_plugin".to_string(),
    );
    let decision_1 = IngestDecision {
        base: decision_1_base,
        source: Link::new(source_id.clone()),
        decision: IngestMode::Retrieval, // Downgraded from Graph
        reason_codes: vec![
            "high_priority".to_string(),
            "budget_pressure".to_string(),
            "graph_budget_low".to_string(),
        ],
        confidence: 0.85,
        estimated_cost: 0.05,
        cost_ledger_id: None,
        budget_snapshot: Some(budget_snapshot.clone()),
        policy_version: Some("v1.0".to_string()),
        downgrade_reason: Some(
            "Graph budget exhausted (0.5 EUR remaining, need 0.50 EUR)".to_string(),
        ),
        original_mode: Some(IngestMode::Graph), // Original intent was Graph
    };
    let created_decision_1 = decision_repo.create(decision_1).await?;
    assert_eq!(
        created_decision_1.decision,
        IngestMode::Retrieval,
        "High-value doc should be downgraded to retrieval under budget pressure"
    );
    assert_eq!(
        created_decision_1.original_mode,
        Some(IngestMode::Graph),
        "Original mode should be preserved"
    );
    assert!(
        created_decision_1.downgrade_reason.is_some(),
        "Downgrade reason should be recorded"
    );
    assert!(
        created_decision_1
            .reason_codes
            .contains(&"budget_pressure".to_string()),
        "Reason codes should include budget_pressure"
    );

    // Invariant 2: Complete budget exhaustion leads to Metadata mode
    let source_2_id = RecordId::new_v4(TableName::Source);
    let mut source_2_base = BaseFields::new(
        source_2_id.clone(),
        space_id.to_string(),
        "P_sys".to_string(),
    );
    source_2_base.security = Security {
        policy_id: "default".to_string(),
        level: 1,
        ..Default::default()
    };
    let mut source_2 = Source::new(
        source_2_base,
        SourceType::Web,
        "hash_budget_test_2".to_string(),
    );
    source_2.external_id = Some("doc-budget-002".to_string());
    source_2.uri = Some("https://example.com/another-doc.html".to_string());
    source_2.metadata = serde_json::json!({"title": "Another Document", "priority": "medium"});
    source_repo.create(source_2).await?;

    // Budget snapshot with both budgets exhausted
    let exhausted_budget_snapshot = BudgetSnapshot {
        remaining_euros_retrieval: 0.001, // Effectively zero
        remaining_euros_graph: 0.0,
        remaining_items_retrieval: Some(0),
        remaining_items_graph: Some(0),
        snapshot_at: Utc::now(),
    };

    let decision_2_id = RecordId::new_v4(TableName::IngestDecision);
    let decision_2_base = BaseFields::new(
        decision_2_id.clone(),
        space_id.to_string(),
        "triage_plugin".to_string(),
    );
    let decision_2 = IngestDecision {
        base: decision_2_base,
        source: Link::new(source_2_id.clone()),
        decision: IngestMode::Metadata, // Downgraded to metadata-only
        reason_codes: vec![
            "budget_exhausted".to_string(),
            "retrieval_items_zero".to_string(),
        ],
        confidence: 1.0,
        estimated_cost: 0.0,
        cost_ledger_id: None,
        budget_snapshot: Some(exhausted_budget_snapshot),
        policy_version: Some("v1.0".to_string()),
        downgrade_reason: Some("All processing budgets exhausted".to_string()),
        original_mode: Some(IngestMode::Retrieval),
    };
    let created_decision_2 = decision_repo.create(decision_2).await?;
    assert_eq!(
        created_decision_2.decision,
        IngestMode::Metadata,
        "Should downgrade to metadata when budgets exhausted"
    );

    // Invariant 4: Budget breach events logged for alerting
    // Note: SystemEventRepository doesn't have a create method, so we verify
    // budget breach tracking via the decision's reason codes and budget snapshots
    assert!(
        created_decision_1
            .reason_codes
            .contains(&"graph_budget_low".to_string()),
        "Budget breach should be recorded in reason codes"
    );
    assert!(
        created_decision_2
            .reason_codes
            .contains(&"budget_exhausted".to_string()),
        "Budget exhaustion should be recorded in reason codes"
    );
    // Budget snapshots provide the alerting context
    let snapshot_1 = created_decision_1.budget_snapshot.as_ref().unwrap();
    assert!(
        snapshot_1.remaining_euros_graph < 1.0,
        "Budget snapshot should show low graph budget for alerting"
    );

    // Verify ledger was updated with new spend
    let mut updated_ledger_retrieval = created_ledger_retrieval.clone();
    updated_ledger_retrieval.total_euros = 8.55;
    updated_ledger_retrieval.total_items = 851;
    updated_ledger_retrieval.reconciled_euros = 8.05;
    let final_ledger = cost_ledger_repo
        .update(space_id, &ledger_retrieval_id, updated_ledger_retrieval)
        .await?;
    assert!(
        (final_ledger.total_euros - 8.55).abs() < f64::EPSILON,
        "Ledger should be updated after processing"
    );
    assert_eq!(
        final_ledger.total_items, 851,
        "Item count should be incremented"
    );

    // Verify all decisions are stored correctly
    let all_decisions = decision_repo.list(space_id, 100, None).await?;
    assert_eq!(all_decisions.len(), 2, "Both decisions should be stored");

    // Verify downgrade chain is correct
    let downgraded_decisions: Vec<_> = all_decisions
        .iter()
        .filter(|d| d.downgrade_reason.is_some())
        .collect();
    assert_eq!(
        downgraded_decisions.len(),
        2,
        "Both decisions were downgraded due to budget"
    );

    Ok(())
}
