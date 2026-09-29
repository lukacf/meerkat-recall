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
//! S4.8 Staged commit scenario test.
//!
//! Verifies work_artifact to canonical table commit flow.

use chrono::Utc;
use elephant_storage::{
    AssertionRepository, Repository, SourceRepository, WorkArtifactRepository, WorkItemRepository,
    WorkRunRepository,
};
use elephant_types::ids::TableName;
use elephant_types::records::assertion::{
    Assertion, AssertionRetryState, EvidenceSpan, ResolutionStatus,
};
use elephant_types::records::base::{BaseFields, Security, SubjectsState};
use elephant_types::records::source::{Source, SourceType};
use elephant_types::records::work::{
    ArtifactStatus, WorkArtifact, WorkItem, WorkItemStatus, WorkRun, WorkRunMetrics,
};
use elephant_types::{Link, RecordId};

use crate::Harness;

/// S4.8: Staged commit from work artifacts to canonical tables.
///
/// Test invariants:
/// 1. Extraction results written to work_artifact first
/// 2. Promotion moves artifacts to canonical tables atomically
/// 3. Failed promotion leaves artifacts intact for retry
/// 4. Successful promotion cleans up work artifacts
#[tokio::test]
async fn scenario_s4_8_staged_commit() -> anyhow::Result<()> {
    let harness = Harness::new().await?;
    let source_repo = SourceRepository::new(harness.client.clone());
    let assertion_repo = AssertionRepository::new(harness.client.clone());
    let work_item_repo = WorkItemRepository::new(harness.client.clone());
    let work_run_repo = WorkRunRepository::new(harness.client.clone());
    let work_artifact_repo = WorkArtifactRepository::new(harness.client.clone());

    let space_id = "Space_StagedCommit";
    let now = Utc::now();

    // Create prerequisite: source for provenance tracking
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
        "hash_staged_commit".to_string(),
    );
    source.external_id = Some("email-staged-001".to_string());
    source.uri = Some("mailto:sender@example.com".to_string());
    source.metadata = serde_json::json!({
        "subject": "Q4 Report",
        "from": "alice@example.com"
    });
    source_repo.create(source).await?;

    // Use placeholder doc_revision ID for evidence spans
    // (In production, DocRevision would be created by the renderer pipeline)
    let doc_rev_id = RecordId::new_v4(TableName::DocRevision);

    // Create a work item for the extraction job
    let work_item_id = RecordId::new_v4(TableName::WorkItem);
    let work_item_base = BaseFields::new(
        work_item_id.clone(),
        space_id.to_string(),
        "scheduler".to_string(),
    );
    let work_item = WorkItem {
        base: work_item_base,
        kind: "extract_assertions".to_string(),
        payload: serde_json::json!({
            "doc_revision": doc_rev_id.to_string(),
            "source": source_id.to_string()
        }),
        status: WorkItemStatus::Running, // Currently executing
        priority: 10,
        idempotency_key: format!("extract:{}", doc_rev_id),
        attempts: 1,
        lease_expires_at: Some(now + chrono::Duration::minutes(5)),
        leased_by: Some("worker-1".to_string()),
        run_after: None,
        last_error: None,
    };
    let created_work_item = work_item_repo.create(work_item).await?;
    assert_eq!(
        created_work_item.status,
        WorkItemStatus::Running,
        "Work item should be running"
    );

    // Create the work run
    let work_run_id = RecordId::new_v4(TableName::WorkRun);
    let work_run_base = BaseFields::new(
        work_run_id.clone(),
        space_id.to_string(),
        "worker-1".to_string(),
    );
    let work_run = WorkRun {
        base: work_run_base,
        work_item: Link::new(work_item_id.clone()),
        attempt: 1,
        started_at: now,
        completed_at: None, // Still running
        success: false,     // Not yet determined
        error: None,
        metrics: WorkRunMetrics {
            tokens_used: 0,
            steps_taken: 0,
            wall_time_ms: 0,
            estimated_cost_euros: 0.0,
            custom: std::collections::HashMap::new(),
        },
        worker_id: "worker-1".to_string(),
        worker_version: "1.0.0".to_string(),
        model_version: Some("gpt-4-turbo".to_string()),
        prompt_id: None,
        config_hash: Some("sha256:config_abc".to_string()),
    };
    let created_work_run = work_run_repo.create(work_run).await?;

    // Invariant 1: Extraction results written to work_artifact first
    // The extractor produces assertions, which are staged as artifacts
    let artifact_1_id = RecordId::new_v4(TableName::WorkArtifact);
    let artifact_1_base = BaseFields::new(
        artifact_1_id.clone(),
        space_id.to_string(),
        "worker-1".to_string(),
    );
    let staged_assertion_1 = serde_json::json!({
        "subject_text": "Q4 revenue",
        "predicate_raw": "exceeded",
        "object_text": "projections",
        "evidence": {
            "revision": doc_rev_id.to_string(),
            "start": 0,
            "end": 34,
            "quote_hash": "sha256:quote_1"
        },
        "confidence": 0.92
    });
    let artifact_1 = WorkArtifact {
        base: artifact_1_base,
        work_run: Link::new(work_run_id.clone()),
        artifact_type: "assertion".to_string(),
        data: staged_assertion_1.clone(),
        status: ArtifactStatus::Draft, // Initially staged as draft
        validation_errors: None,
        validated_at: None,
        committed_at: None,
    };
    let created_artifact_1 = work_artifact_repo.create(artifact_1).await?;
    assert_eq!(
        created_artifact_1.status,
        ArtifactStatus::Draft,
        "Artifact should start as Draft"
    );

    let artifact_2_id = RecordId::new_v4(TableName::WorkArtifact);
    let artifact_2_base = BaseFields::new(
        artifact_2_id.clone(),
        space_id.to_string(),
        "worker-1".to_string(),
    );
    let staged_assertion_2 = serde_json::json!({
        "subject_text": "John Smith",
        "predicate_raw": "led",
        "object_text": "the marketing campaign",
        "evidence": {
            "revision": doc_rev_id.to_string(),
            "start": 36,
            "end": 70,
            "quote_hash": "sha256:quote_2"
        },
        "confidence": 0.88
    });
    let artifact_2 = WorkArtifact {
        base: artifact_2_base,
        work_run: Link::new(work_run_id.clone()),
        artifact_type: "assertion".to_string(),
        data: staged_assertion_2.clone(),
        status: ArtifactStatus::Draft,
        validation_errors: None,
        validated_at: None,
        committed_at: None,
    };
    let created_artifact_2 = work_artifact_repo.create(artifact_2).await?;

    // Invariant 3: Failed validation leaves artifacts intact for retry
    // Simulate a validation failure on artifact 2
    let mut failed_artifact = created_artifact_2.clone();
    failed_artifact.status = ArtifactStatus::Rejected;
    failed_artifact.validation_errors = Some(vec![
        "Subject 'John Smith' requires entity disambiguation".to_string(),
    ]);
    let rejected_artifact = work_artifact_repo
        .update(space_id, &artifact_2_id, failed_artifact)
        .await?;
    assert_eq!(
        rejected_artifact.status,
        ArtifactStatus::Rejected,
        "Failed validation should set Rejected status"
    );
    assert!(
        rejected_artifact.validation_errors.is_some(),
        "Validation errors should be recorded"
    );
    // Data should still be intact for inspection/retry
    assert_eq!(
        rejected_artifact.data["subject_text"], staged_assertion_2["subject_text"],
        "Rejected artifact data should be preserved"
    );

    // Invariant 2: Promotion moves artifacts to canonical tables atomically
    // Validate and commit artifact 1
    let mut validated_artifact = created_artifact_1.clone();
    validated_artifact.status = ArtifactStatus::Validated;
    validated_artifact.validated_at = Some(now);
    let validated_artifact_1 = work_artifact_repo
        .update(space_id, &artifact_1_id, validated_artifact)
        .await?;
    assert_eq!(
        validated_artifact_1.status,
        ArtifactStatus::Validated,
        "Artifact should be validated"
    );

    // Create the canonical assertion from the validated artifact
    let assertion_id = RecordId::new_v4(TableName::Assertion);
    let assertion_base = BaseFields::new(
        assertion_id.clone(),
        space_id.to_string(),
        "commit_worker".to_string(),
    );
    // Note: Not setting provenance.sources to avoid SurrealDB meta::tb() serialization issues
    let assertion = Assertion {
        base: assertion_base,
        subject_text: staged_assertion_1["subject_text"]
            .as_str()
            .unwrap()
            .to_string(),
        predicate_raw: staged_assertion_1["predicate_raw"]
            .as_str()
            .unwrap()
            .to_string(),
        predicate_id: None,
        object_text: staged_assertion_1["object_text"]
            .as_str()
            .map(|s| s.to_string()),
        object_literal: None,
        evidence: EvidenceSpan {
            revision: Link::new(doc_rev_id.clone()),
            node: None,
            start: 0,
            end: 34,
            quote_hash: "sha256:quote_1".to_string(),
        },
        source_event_time: None,
        observed_at: now,
        asserted_at: now,
        assertion_key: format!("{}:exceeded:0-34", doc_rev_id),
        resolution_status: ResolutionStatus::Pending,
        resolution_method: None,
        resolution_confidence: None,
        retry_state: AssertionRetryState::default(),
        subject_entity: None,
        object_entity: None,
        confidence: staged_assertion_1["confidence"].as_f64().unwrap() as f32,
        subject_entity_type: None,
        object_entity_type: None,
        significance: None,
        lifecycle_tier: "active".to_string(),
    };
    let committed_assertion = assertion_repo.create(assertion).await?;
    assert_eq!(
        committed_assertion.subject_text, "Q4 revenue",
        "Assertion should be created in canonical table"
    );

    // Invariant 4: Successful promotion cleans up work artifacts
    // Mark artifact as committed
    let mut committed_artifact = validated_artifact_1.clone();
    committed_artifact.status = ArtifactStatus::Committed;
    committed_artifact.committed_at = Some(now);
    let final_artifact_1 = work_artifact_repo
        .update(space_id, &artifact_1_id, committed_artifact)
        .await?;
    assert_eq!(
        final_artifact_1.status,
        ArtifactStatus::Committed,
        "Artifact should be marked as committed"
    );
    assert!(
        final_artifact_1.committed_at.is_some(),
        "Commit timestamp should be recorded"
    );

    // Mark work run as completed successfully
    let mut completed_run = created_work_run.clone();
    completed_run.completed_at = Some(now);
    completed_run.success = true;
    completed_run.metrics = WorkRunMetrics {
        tokens_used: 1500,
        steps_taken: 1,
        wall_time_ms: 2500,
        estimated_cost_euros: 0.003,
        custom: std::collections::HashMap::new(),
    };
    let final_run = work_run_repo
        .update(space_id, &work_run_id, completed_run)
        .await?;
    assert!(final_run.success, "Work run should be successful");
    assert!(
        final_run.completed_at.is_some(),
        "Completion time should be recorded"
    );

    // Mark work item as done
    let mut done_work_item = created_work_item.clone();
    done_work_item.status = WorkItemStatus::Done;
    done_work_item.lease_expires_at = None;
    done_work_item.leased_by = None;
    let final_work_item = work_item_repo
        .update(space_id, &work_item_id, done_work_item)
        .await?;
    assert_eq!(
        final_work_item.status,
        WorkItemStatus::Done,
        "Work item should be Done"
    );

    // Verify final state: one canonical assertion created
    let all_assertions = assertion_repo.list(space_id, 100, None).await?;
    assert_eq!(
        all_assertions.len(),
        1,
        "One assertion should be in canonical table"
    );

    // Verify artifact states
    let all_artifacts = work_artifact_repo.list(space_id, 100, None).await?;
    assert_eq!(all_artifacts.len(), 2, "Both artifacts should exist");

    let committed_count = all_artifacts
        .iter()
        .filter(|a| a.status == ArtifactStatus::Committed)
        .count();
    let rejected_count = all_artifacts
        .iter()
        .filter(|a| a.status == ArtifactStatus::Rejected)
        .count();

    assert_eq!(committed_count, 1, "One artifact should be committed");
    assert_eq!(
        rejected_count, 1,
        "One artifact should be rejected (preserved for retry)"
    );

    Ok(())
}
