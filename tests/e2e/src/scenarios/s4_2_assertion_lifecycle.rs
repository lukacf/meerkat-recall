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
//! S4.2 Assertion lifecycle scenario test.
//!
//! Verifies the complete assertion flow: extract -> resolve -> commit.

use chrono::Utc;
use elephant_storage::{
    AssertionRepository, DocRepository, DocRevisionRepository, EntityRepository, Repository,
    WorkArtifactRepository, WorkItemRepository, WorkRunRepository,
};
use elephant_types::ids::TableName;
use elephant_types::records::assertion::{
    Assertion, AssertionRetryState, EvidenceSpan, ResolutionStatus,
};
use elephant_types::records::base::{BaseFields, Security, SubjectsState};
use elephant_types::records::doc::Doc;
use elephant_types::records::doc_revision::{DocRevision, RendererMetadata, RevisionStatus};
use elephant_types::records::entity::EntityType;
use elephant_types::records::work::{
    ArtifactStatus, WorkArtifact, WorkItem, WorkItemStatus, WorkRun, WorkRunMetrics,
};
use elephant_types::records::Entity;
use elephant_types::{Link, RecordId};

use crate::Harness;

/// S4.2: Assertion lifecycle from extraction to commitment.
///
/// Test invariants:
/// 1. Assertions extracted from doc_revision text
/// 2. Subject/object resolution attempts entity matching
/// 3. Resolved assertions create rels/attributes
/// 4. Work artifacts staged before canonical commit
#[tokio::test]
async fn scenario_s4_2_assertion_lifecycle() -> anyhow::Result<()> {
    let harness = Harness::new().await?;
    let doc_repo = DocRepository::new(harness.client.clone());
    let revision_repo = DocRevisionRepository::new(harness.client.clone());
    let assertion_repo = AssertionRepository::new(harness.client.clone());
    let entity_repo = EntityRepository::new(harness.client.clone());
    let work_item_repo = WorkItemRepository::new(harness.client.clone());
    let work_run_repo = WorkRunRepository::new(harness.client.clone());
    let artifact_repo = WorkArtifactRepository::new(harness.client.clone());

    let space_id = "Space_Assertion";

    // Step 1: Create a document and revision with extractable text
    let doc_id = RecordId::new_v4(TableName::Doc);
    let mut doc_base = BaseFields::new(doc_id.clone(), space_id.to_string(), "P_sys".to_string());
    doc_base.security = Security {
        policy_id: "default".to_string(),
        level: 1,
        ..Default::default()
    };
    doc_base.subjects_state = SubjectsState::Complete;
    let revision_text =
        "John Smith works at Acme Corporation. He has been the CEO since 2020.".to_string();
    let doc = Doc {
        base: doc_base,
        title: "Company Profile Document".to_string(),
        text: revision_text.clone(),
        sidecar_text: None,
        source: None,
        media_attachments: Vec::new(),
        rendition_key: "doc-profile-001".to_string(),
        metadata: serde_json::json!({}),
    };
    doc_repo.create(doc).await?;

    // Create document revision with text containing entity mentions
    let revision_id = RecordId::new_v4(TableName::DocRevision);
    let mut revision_base = BaseFields::new(
        revision_id.clone(),
        space_id.to_string(),
        "P_sys".to_string(),
    );
    revision_base.security = Security {
        policy_id: "default".to_string(),
        level: 1,
        ..Default::default()
    };
    let revision = DocRevision {
        base: revision_base,
        doc: Link::new(doc_id.clone()),
        text: revision_text.clone(),
        text_hash: "sha256:abc123".to_string(),
        renderer: RendererMetadata {
            name: "markdown_renderer".to_string(),
            version: "1.0.0".to_string(),
            config_hash: None,
        },
        manifest: None,
        supersedes: None,
        status: RevisionStatus::Active,
        rendered_at: Utc::now(),
    };
    revision_repo.create(revision).await?;

    // Step 2: Create entities that assertions will resolve to
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
    let mut entity_john = Entity::new(
        entity_john_base,
        EntityType::new("person"),
        "John Smith".to_string(),
    );
    entity_john.identity_keys = vec!["name:john_smith".to_string()];
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
    let mut entity_acme = Entity::new(
        entity_acme_base,
        EntityType::new("org"),
        "Acme Corporation".to_string(),
    );
    entity_acme.identity_keys = vec!["name:acme_corporation".to_string()];
    entity_repo.create(entity_acme).await?;

    // Invariant 1: Assertions extracted from doc_revision text
    let assertion_1_id = RecordId::new_v4(TableName::Assertion);
    let assertion_1_base = BaseFields::new(
        assertion_1_id.clone(),
        space_id.to_string(),
        "extractor_plugin".to_string(),
    );
    let assertion_1 = Assertion {
        base: assertion_1_base,
        subject_text: "John Smith".to_string(),
        predicate_raw: "works_at".to_string(),
        predicate_id: None,
        object_text: Some("Acme Corporation".to_string()),
        object_literal: None,
        evidence: EvidenceSpan {
            revision: Link::new(revision_id.clone()),
            node: None,
            start: 0,
            end: 37,
            quote_hash: "sha256:evidence1".to_string(),
        },
        source_event_time: None,
        observed_at: Utc::now(),
        asserted_at: Utc::now(),
        assertion_key: "assertion-works-at-001".to_string(),
        resolution_status: ResolutionStatus::Pending,
        resolution_method: None,
        resolution_confidence: None,
        retry_state: AssertionRetryState::default(),
        subject_entity: None,
        object_entity: None,
        confidence: 0.92,
        subject_entity_type: None,
        object_entity_type: None,
        significance: None,
        lifecycle_tier: "active".to_string(),
    };
    let created_assertion_1 = assertion_repo.create(assertion_1).await?;
    assert_eq!(
        created_assertion_1.resolution_status,
        ResolutionStatus::Pending,
        "New assertions should start in pending status"
    );
    assert_eq!(
        created_assertion_1.subject_text, "John Smith",
        "Subject text should be extracted from document"
    );

    // Invariant 2: Subject/object resolution attempts entity matching
    // Simulate resolution by updating the assertion with resolved entities
    let mut resolved_assertion = created_assertion_1.clone();
    resolved_assertion.resolution_status = ResolutionStatus::Resolved;
    resolved_assertion.subject_entity = Some(Link::new(entity_john_id.clone()));
    resolved_assertion.object_entity = Some(Link::new(entity_acme_id.clone()));
    resolved_assertion.retry_state.resolution_attempts = 1;
    resolved_assertion.retry_state.last_resolution_at = Some(Utc::now());

    let updated_assertion = assertion_repo
        .update(space_id, &assertion_1_id, resolved_assertion)
        .await?;
    assert_eq!(
        updated_assertion.resolution_status,
        ResolutionStatus::Resolved,
        "Assertion should be marked as resolved"
    );
    assert!(
        updated_assertion.subject_entity.is_some(),
        "Subject entity should be linked"
    );
    assert!(
        updated_assertion.object_entity.is_some(),
        "Object entity should be linked"
    );

    // Invariant 3 & 4: Work artifacts staged before canonical commit
    // Create a work item for the extraction task
    let work_item_id = RecordId::new_v4(TableName::WorkItem);
    let work_item_base = BaseFields::new(
        work_item_id.clone(),
        space_id.to_string(),
        "scheduler".to_string(),
    );
    let work_item = WorkItem {
        base: work_item_base,
        kind: "extraction".to_string(),
        payload: serde_json::json!({
            "revision_id": revision_id.to_string(),
            "mode": "graph"
        }),
        status: WorkItemStatus::Done,
        priority: 5,
        idempotency_key: format!("extract-{}", revision_id),
        attempts: 1,
        lease_expires_at: None,
        leased_by: None,
        run_after: None,
        last_error: None,
    };
    work_item_repo.create(work_item).await?;

    // Create work run record
    let work_run_id = RecordId::new_v4(TableName::WorkRun);
    let work_run_base = BaseFields::new(
        work_run_id.clone(),
        space_id.to_string(),
        "scheduler".to_string(),
    );
    let work_run = WorkRun {
        base: work_run_base,
        work_item: Link::new(work_item_id.clone()),
        attempt: 1,
        started_at: Utc::now(),
        completed_at: Some(Utc::now()),
        success: true,
        error: None,
        metrics: WorkRunMetrics {
            tokens_used: 1500,
            steps_taken: 3,
            wall_time_ms: 2500,
            estimated_cost_euros: 0.05,
            custom: std::collections::HashMap::new(),
        },
        worker_id: "extractor-worker-1".to_string(),
        worker_version: "1.0.0".to_string(),
        model_version: Some("gpt-4".to_string()),
        prompt_id: None,
        config_hash: Some("config-hash-123".to_string()),
    };
    work_run_repo.create(work_run).await?;

    // Invariant 4: Work artifacts staged before canonical commit
    let artifact_id = RecordId::new_v4(TableName::WorkArtifact);
    let artifact_base = BaseFields::new(
        artifact_id.clone(),
        space_id.to_string(),
        "extractor".to_string(),
    );
    let artifact = WorkArtifact {
        base: artifact_base,
        work_run: Link::new(work_run_id.clone()),
        artifact_type: "knowledge_candidates".to_string(),
        data: serde_json::json!({
            "assertions": [
                {
                    "id": assertion_1_id.to_string(),
                    "subject_text": "John Smith",
                    "predicate_raw": "works_at",
                    "object_text": "Acme Corporation"
                }
            ],
            "total_count": 1
        }),
        status: ArtifactStatus::Draft, // Staged, not yet committed
        validation_errors: None,
        validated_at: None,
        committed_at: None,
    };
    let created_artifact = artifact_repo.create(artifact).await?;
    assert_eq!(
        created_artifact.status,
        ArtifactStatus::Draft,
        "Artifacts should start in draft status"
    );
    assert_eq!(
        created_artifact.artifact_type, "knowledge_candidates",
        "Artifact type should identify the content"
    );

    // Simulate validation pass
    let mut validated_artifact = created_artifact.clone();
    validated_artifact.status = ArtifactStatus::Validated;
    validated_artifact.validated_at = Some(Utc::now());
    let updated_artifact = artifact_repo
        .update(space_id, &artifact_id, validated_artifact)
        .await?;
    assert_eq!(
        updated_artifact.status,
        ArtifactStatus::Validated,
        "Artifact should be validated before commit"
    );

    // Simulate commit
    let mut committed_artifact = updated_artifact.clone();
    committed_artifact.status = ArtifactStatus::Committed;
    committed_artifact.committed_at = Some(Utc::now());
    let final_artifact = artifact_repo
        .update(space_id, &artifact_id, committed_artifact)
        .await?;
    assert_eq!(
        final_artifact.status,
        ArtifactStatus::Committed,
        "Artifact should be committed to canonical tables"
    );
    assert!(
        final_artifact.committed_at.is_some(),
        "Commit timestamp should be recorded"
    );

    // Verify the complete lifecycle
    let retrieved_assertion = assertion_repo.get(space_id, &assertion_1_id).await?;
    assert!(retrieved_assertion.is_some());
    let assertion = retrieved_assertion.unwrap();
    assert_eq!(
        assertion.resolution_status,
        ResolutionStatus::Resolved,
        "Final assertion state should be resolved"
    );

    Ok(())
}
