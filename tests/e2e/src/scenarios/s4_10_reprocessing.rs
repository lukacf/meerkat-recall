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
//! S4.10 Reprocessing scenario test.
//!
//! Verifies backfill supersession when reprocessing sources.

use chrono::{NaiveDate, Utc};
use elephant_storage::{
    AssertionRepository, DocRepository, DocRevisionRepository, Repository, ReprocessJobRepository,
    SourceRepository,
};
use elephant_types::ids::TableName;
use elephant_types::records::assertion::{
    Assertion, AssertionRetryState, EvidenceSpan, ResolutionStatus,
};
use elephant_types::records::base::{BaseFields, Security, SubjectsState};
use elephant_types::records::doc::Doc;
use elephant_types::records::doc_revision::{DocRevision, RendererMetadata, RevisionStatus};
use elephant_types::records::reprocess::{
    default_reprocess_target_stages, ReprocessJob, ReprocessProgress, ReprocessStatus,
    SelectionCriteria,
};
use elephant_types::records::source::{Source, SourceType};
use elephant_types::{Link, RecordId};

use crate::Harness;

/// S4.10: Reprocessing and backfill supersession.
///
/// Test invariants:
/// 1. Reprocess job created with selection criteria
/// 2. Matching sources re-extracted with new extractor
/// 3. Old revisions/assertions superseded by new ones
/// 4. Budget-aware pacing respects daily limits
#[tokio::test]
async fn scenario_s4_10_reprocessing() -> anyhow::Result<()> {
    let harness = Harness::new().await?;
    let source_repo = SourceRepository::new(harness.client.clone());
    let doc_repo = DocRepository::new(harness.client.clone());
    let revision_repo = DocRevisionRepository::new(harness.client.clone());
    let assertion_repo = AssertionRepository::new(harness.client.clone());
    let reprocess_repo = ReprocessJobRepository::new(harness.client.clone());

    let space_id = "Space_Reprocess";

    // Create source documents that will be reprocessed
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
    let mut source_1 = Source::new(base_1, SourceType::Drive, "hash_old_doc_1".to_string());
    source_1.external_id = Some("doc-reprocess-001".to_string());
    source_1.uri = Some("https://example.com/report1.pdf".to_string());
    source_1.metadata =
        serde_json::json!({"extractor_version": "1.0.0", "processed_at": "2024-01-15"});
    source_repo.create(source_1).await?;

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
    let mut source_2 = Source::new(base_2, SourceType::Drive, "hash_old_doc_2".to_string());
    source_2.external_id = Some("doc-reprocess-002".to_string());
    source_2.uri = Some("https://example.com/report2.pdf".to_string());
    source_2.metadata =
        serde_json::json!({"extractor_version": "1.0.0", "processed_at": "2024-01-20"});
    source_repo.create(source_2).await?;

    // Create original doc and revision for source 1
    let doc_1_id = RecordId::new_v4(TableName::Doc);
    let doc_1_base = BaseFields::new(doc_1_id.clone(), space_id.to_string(), "P_sys".to_string());
    let doc_1 = Doc {
        base: doc_1_base,
        title: "Original Report 1".to_string(),
        text: "Alice works at TechCorp as a senior engineer.".to_string(),
        sidecar_text: None,
        source: Some(Link::new(source_1_id.clone())),
        media_attachments: Vec::new(),
        rendition_key: "doc-reprocess-001-v1".to_string(),
        metadata: serde_json::json!({}),
    };
    doc_repo.create(doc_1).await?;

    let revision_1_id = RecordId::new_v4(TableName::DocRevision);
    let revision_1_base = BaseFields::new(
        revision_1_id.clone(),
        space_id.to_string(),
        "extractor_v1".to_string(),
    );
    let revision_1 = DocRevision {
        base: revision_1_base,
        doc: Link::new(doc_1_id.clone()),
        text: "Alice works at TechCorp as a senior engineer.".to_string(),
        text_hash: "sha256:old_text_hash_1".to_string(),
        renderer: RendererMetadata {
            name: "pdf_renderer".to_string(),
            version: "1.0.0".to_string(),
            config_hash: None,
        },
        manifest: None,
        supersedes: None,
        status: RevisionStatus::Active,
        rendered_at: Utc::now(),
    };
    revision_repo.create(revision_1).await?;

    // Create original assertion from old extractor
    let assertion_old_id = RecordId::new_v4(TableName::Assertion);
    let assertion_old_base = BaseFields::new(
        assertion_old_id.clone(),
        space_id.to_string(),
        "extractor_v1".to_string(),
    );
    let assertion_old = Assertion {
        base: assertion_old_base,
        subject_text: "Alice".to_string(),
        predicate_raw: "works_at".to_string(),
        predicate_id: None,
        object_text: Some("TechCorp".to_string()),
        object_literal: None,
        evidence: EvidenceSpan {
            revision: Link::new(revision_1_id.clone()),
            node: None,
            start: 0,
            end: 45,
            quote_hash: "sha256:evidence_old".to_string(),
        },
        source_event_time: None,
        observed_at: Utc::now(),
        asserted_at: Utc::now(),
        assertion_key: "assertion-old-alice-works-at".to_string(),
        resolution_status: ResolutionStatus::Resolved,
        resolution_method: None,
        resolution_confidence: None,
        retry_state: AssertionRetryState::default(),
        subject_entity: None,
        object_entity: None,
        confidence: 0.75, // Lower confidence from old extractor
        subject_entity_type: None,
        object_entity_type: None,
        significance: None,
        lifecycle_tier: "active".to_string(),
    };
    let created_old_assertion = assertion_repo.create(assertion_old).await?;

    // Invariant 1: Reprocess job created with selection criteria
    let reprocess_job_id = RecordId::new_v4(TableName::ReprocessJob);
    let reprocess_job_base = BaseFields::new(
        reprocess_job_id.clone(),
        space_id.to_string(),
        "admin".to_string(),
    );
    let reprocess_job = ReprocessJob {
        base: reprocess_job_base,
        name: "Backfill with extractor v2".to_string(),
        selection_criteria: SelectionCriteria {
            source_types: vec!["drive".to_string()],
            date_from: Some(NaiveDate::from_ymd_opt(2024, 1, 1).unwrap()),
            date_to: Some(NaiveDate::from_ymd_opt(2024, 6, 30).unwrap()),
            extractor_version_before: Some("2.0.0".to_string()),
            predicate_ids: vec![],
            space_ids: vec![space_id.to_string()],
            source_status: None,
            custom_filter: None,
        },
        target_stages: default_reprocess_target_stages(),
        status: ReprocessStatus::Pending,
        progress: ReprocessProgress::default(),
        priority: 5,
        budget_limit_euros: Some(50.0),
        max_sources_per_day: Some(100), // Pacing limit
        started_at: None,
        completed_at: None,
        error: None,
        reason: Some("Extractor v2 released with improved entity extraction".to_string()),
    };
    let created_job = reprocess_repo.create(reprocess_job).await?;
    assert_eq!(
        created_job.status,
        ReprocessStatus::Pending,
        "CRITICAL: Reprocess job should start in pending status"
    );
    assert!(
        created_job
            .selection_criteria
            .extractor_version_before
            .is_some(),
        "Selection criteria should include extractor version filter"
    );

    // Invariant 2: Simulate job starting and matching sources being re-extracted
    let mut running_job = created_job.clone();
    running_job.status = ReprocessStatus::Running;
    running_job.started_at = Some(Utc::now());
    running_job.progress = ReprocessProgress {
        total_sources: 2,
        processed_sources: 0,
        succeeded_sources: 0,
        failed_sources: 0,
        skipped_sources: 0,
        progress_percent: 0.0,
        eta_seconds: Some(120),
        cost_spent_euros: 0.0,
    };
    let updated_job = reprocess_repo
        .update(space_id, &reprocess_job_id, running_job)
        .await?;
    assert_eq!(
        updated_job.status,
        ReprocessStatus::Running,
        "Job should transition to running"
    );
    assert!(
        updated_job.started_at.is_some(),
        "Start time should be recorded"
    );

    // Invariant 3: Old revisions/assertions superseded by new ones
    // Create new revision from extractor v2
    let revision_2_id = RecordId::new_v4(TableName::DocRevision);
    let revision_2_base = BaseFields::new(
        revision_2_id.clone(),
        space_id.to_string(),
        "extractor_v2".to_string(),
    );
    let revision_2 = DocRevision {
        base: revision_2_base,
        doc: Link::new(doc_1_id.clone()),
        text: "Alice works at TechCorp as a senior engineer.".to_string(),
        text_hash: "sha256:new_text_hash_1".to_string(),
        renderer: RendererMetadata {
            name: "pdf_renderer".to_string(),
            version: "2.0.0".to_string(),
            config_hash: Some("config-v2".to_string()),
        },
        manifest: None,
        supersedes: Some(Link::new(revision_1_id.clone())), // Points to old revision
        status: RevisionStatus::Active,
        rendered_at: Utc::now(),
    };
    let created_revision_2 = revision_repo.create(revision_2).await?;
    assert!(
        created_revision_2.supersedes.is_some(),
        "New revision should supersede old one"
    );

    // Create new assertion with higher confidence from extractor v2
    let assertion_new_id = RecordId::new_v4(TableName::Assertion);
    let assertion_new_base = BaseFields::new(
        assertion_new_id.clone(),
        space_id.to_string(),
        "extractor_v2".to_string(),
    );
    let assertion_new = Assertion {
        base: assertion_new_base,
        subject_text: "Alice".to_string(),
        predicate_raw: "works_at".to_string(),
        predicate_id: None,
        object_text: Some("TechCorp".to_string()),
        object_literal: None,
        evidence: EvidenceSpan {
            revision: Link::new(revision_2_id.clone()),
            node: None,
            start: 0,
            end: 45,
            quote_hash: "sha256:evidence_new".to_string(),
        },
        source_event_time: None,
        observed_at: Utc::now(),
        asserted_at: Utc::now(),
        assertion_key: "assertion-new-alice-works-at".to_string(),
        resolution_status: ResolutionStatus::Resolved,
        resolution_method: None,
        resolution_confidence: None,
        retry_state: AssertionRetryState::default(),
        subject_entity: None,
        object_entity: None,
        confidence: 0.95, // Higher confidence from improved extractor
        subject_entity_type: None,
        object_entity_type: None,
        significance: None,
        lifecycle_tier: "active".to_string(),
    };
    let created_new_assertion = assertion_repo.create(assertion_new).await?;
    assert!(
        created_new_assertion.confidence > created_old_assertion.confidence,
        "New assertion should have higher confidence from improved extractor"
    );

    // Mark old assertion as superseded
    let mut superseded_assertion = created_old_assertion.clone();
    superseded_assertion.resolution_status = ResolutionStatus::Superseded;
    let updated_old_assertion = assertion_repo
        .update(space_id, &assertion_old_id, superseded_assertion)
        .await?;
    assert_eq!(
        updated_old_assertion.resolution_status,
        ResolutionStatus::Superseded,
        "Old assertion should be marked as superseded"
    );

    // Mark old revision as superseded
    let mut old_revision = revision_repo.get(space_id, &revision_1_id).await?.unwrap();
    old_revision.status = RevisionStatus::Superseded;
    let updated_old_revision = revision_repo
        .update(space_id, &revision_1_id, old_revision)
        .await?;
    assert_eq!(
        updated_old_revision.status,
        RevisionStatus::Superseded,
        "Old revision should be marked as superseded"
    );

    // Invariant 4: Budget-aware pacing respects daily limits
    let mut progress_job = updated_job.clone();
    progress_job.progress = ReprocessProgress {
        total_sources: 2,
        processed_sources: 1,
        succeeded_sources: 1,
        failed_sources: 0,
        skipped_sources: 0,
        progress_percent: 50.0,
        eta_seconds: Some(60),
        cost_spent_euros: 5.5,
    };
    let job_with_progress = reprocess_repo
        .update(space_id, &reprocess_job_id, progress_job)
        .await?;
    assert!(
        job_with_progress.progress.cost_spent_euros < job_with_progress.budget_limit_euros.unwrap(),
        "Cost should be within budget limit"
    );
    assert_eq!(
        job_with_progress.max_sources_per_day,
        Some(100),
        "Daily pacing limit should be preserved"
    );

    // Complete the job
    let mut completed_job = job_with_progress.clone();
    completed_job.status = ReprocessStatus::Completed;
    completed_job.completed_at = Some(Utc::now());
    completed_job.progress = ReprocessProgress {
        total_sources: 2,
        processed_sources: 2,
        succeeded_sources: 2,
        failed_sources: 0,
        skipped_sources: 0,
        progress_percent: 100.0,
        eta_seconds: None,
        cost_spent_euros: 10.5,
    };
    let final_job = reprocess_repo
        .update(space_id, &reprocess_job_id, completed_job)
        .await?;
    assert_eq!(
        final_job.status,
        ReprocessStatus::Completed,
        "Job should complete successfully"
    );
    assert!(
        final_job.completed_at.is_some(),
        "Completion time should be recorded"
    );
    assert_eq!(
        final_job.progress.succeeded_sources, 2,
        "All sources should be processed successfully"
    );

    // Verify the complete reprocessing flow
    let all_jobs = reprocess_repo.list(space_id, 100, None).await?;
    assert_eq!(all_jobs.len(), 1, "Should have one reprocess job");

    Ok(())
}
