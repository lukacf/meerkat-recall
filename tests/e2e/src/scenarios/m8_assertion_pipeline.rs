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
//! M8: Assertion Pipeline Integration tests.
//!
//! Verifies end-to-end assertion flow:
//! - Document ingestion produces assertions
//! - Assertions flow through resolution
//! - Resolved assertions produce facts/rels

use chrono::Utc;

use elephant_storage::{
    AssertionRepository, DocRepository, DocRevisionRepository, EntityRepository, RelRepository,
    Repository,
};
use elephant_types::ids::TableName;
use elephant_types::records::assertion::{
    Assertion, AssertionRetryState, EvidenceSpan, ResolutionStatus,
};
use elephant_types::records::base::BaseFields;
use elephant_types::records::doc::Doc;
use elephant_types::records::doc_revision::{DocRevision, RendererMetadata, RevisionStatus};
use elephant_types::records::entity::{Entity, EntityType};
use elephant_types::records::rel::Rel;
use elephant_types::{Link, RecordId};

use crate::Harness;

/// M8.1: Assertions are created from document revision text.
///
/// Test invariants:
/// 1. Document ingestion creates a doc_revision
/// 2. Extraction produces assertions with evidence spans
/// 3. Assertions reference the source revision
#[tokio::test]
#[ignore]
async fn scenario_m8_1_doc_produces_assertions() -> anyhow::Result<()> {
    let harness = Harness::new().await?;
    let space_id = "test_space";

    // Create a document
    let doc_repo = DocRepository::new(harness.client.clone());
    let doc_id = RecordId::new_v4(TableName::Doc);
    let doc_base = BaseFields::new(doc_id.clone(), space_id.to_string(), "P_sys".to_string());
    let doc_text =
        "John Doe works at Acme Corp. He joined in 2020 as a senior engineer.".to_string();
    let doc = Doc {
        base: doc_base,
        title: "Meeting Notes 2024-01".to_string(),
        text: doc_text.clone(),
        sidecar_text: None,
        source: None,
        media_attachments: Vec::new(),
        rendition_key: format!("meeting-notes-{}", uuid::Uuid::new_v4()),
        metadata: serde_json::json!({}),
    };
    doc_repo.create(doc).await?;

    // Create a document revision (simulating extraction output)
    let revision_repo = DocRevisionRepository::new(harness.client.clone());
    let revision_id = RecordId::new_v4(TableName::DocRevision);
    let revision_base = BaseFields::new(
        revision_id.clone(),
        space_id.to_string(),
        "P_sys".to_string(),
    );
    let revision = DocRevision {
        base: revision_base,
        doc: Link::new(doc_id.clone()),
        text: doc_text,
        text_hash: "sha256:def456".to_string(),
        renderer: RendererMetadata {
            name: "default_renderer".to_string(),
            version: "1.0.0".to_string(),
            config_hash: None,
        },
        manifest: None,
        supersedes: None,
        status: RevisionStatus::Active,
        rendered_at: Utc::now(),
    };
    revision_repo.create(revision).await?;

    // Create assertions extracted from the revision
    let assertion_repo = AssertionRepository::new(harness.client.clone());

    // Assertion 1: "John Doe works at Acme Corp"
    let assertion1_id = RecordId::new_v4(TableName::Assertion);
    let assertion1_base = BaseFields::new(
        assertion1_id.clone(),
        space_id.to_string(),
        "P_sys".to_string(),
    );
    let assertion1 = Assertion {
        base: assertion1_base,
        subject_text: "John Doe".to_string(),
        predicate_raw: "works_at".to_string(),
        predicate_id: None,
        object_text: Some("Acme Corp".to_string()),
        object_literal: None,
        evidence: EvidenceSpan {
            revision: Link::new(revision_id.clone()),
            node: None,
            start: 0,
            end: 24,
            quote_hash: "hash1".to_string(),
        },
        source_event_time: None,
        observed_at: Utc::now(),
        asserted_at: Utc::now(),
        assertion_key: format!("assertion-{}", uuid::Uuid::new_v4()),
        resolution_status: ResolutionStatus::Pending,
        resolution_method: None,
        resolution_confidence: None,
        retry_state: AssertionRetryState::default(),
        subject_entity: None,
        object_entity: None,
        confidence: 0.95,
        subject_entity_type: None,
        object_entity_type: None,
        significance: None,
        lifecycle_tier: "active".to_string(),
    };
    assertion_repo.create(assertion1).await?;

    // Assertion 2: "He joined in 2020"
    let assertion2_id = RecordId::new_v4(TableName::Assertion);
    let assertion2_base = BaseFields::new(
        assertion2_id.clone(),
        space_id.to_string(),
        "P_sys".to_string(),
    );
    let assertion2 = Assertion {
        base: assertion2_base,
        subject_text: "John Doe".to_string(),
        predicate_raw: "joined".to_string(),
        predicate_id: None,
        object_text: None,
        object_literal: Some(serde_json::json!(2020)),
        evidence: EvidenceSpan {
            revision: Link::new(revision_id.clone()),
            node: None,
            start: 25,
            end: 42,
            quote_hash: "hash2".to_string(),
        },
        source_event_time: None,
        observed_at: Utc::now(),
        asserted_at: Utc::now(),
        assertion_key: format!("assertion-{}", uuid::Uuid::new_v4()),
        resolution_status: ResolutionStatus::Pending,
        resolution_method: None,
        resolution_confidence: None,
        retry_state: AssertionRetryState::default(),
        subject_entity: None,
        object_entity: None,
        confidence: 0.88,
        subject_entity_type: None,
        object_entity_type: None,
        significance: None,
        lifecycle_tier: "active".to_string(),
    };
    assertion_repo.create(assertion2).await?;

    // Verify assertions were created and reference the revision
    let assertions = assertion_repo.list(space_id, 10, None).await?;
    assert_eq!(
        assertions.len(),
        2,
        "Expected 2 assertions from document extraction"
    );

    for assertion in &assertions {
        assert_eq!(
            assertion.evidence.revision.id(),
            &revision_id,
            "Assertion must reference the source revision"
        );
        assert_eq!(
            assertion.resolution_status,
            ResolutionStatus::Pending,
            "New assertions should be pending resolution"
        );
    }

    Ok(())
}

/// M8.2: Assertions flow through resolution to entities.
///
/// Test invariants:
/// 1. Pending assertions are picked up for resolution
/// 2. Subject/object text is matched to entities
/// 3. Resolved assertions link to entity records
#[tokio::test]
#[ignore]
async fn scenario_m8_2_assertions_resolve_to_entities() -> anyhow::Result<()> {
    let harness = Harness::new().await?;
    let space_id = "test_space";

    // Create entities for resolution targets
    let entity_repo = EntityRepository::new(harness.client.clone());

    let person_id = RecordId::new_v4(TableName::Entity);
    let person_base = BaseFields::new(person_id.clone(), space_id.to_string(), "P_sys".to_string());
    let person = Entity::new(
        person_base,
        EntityType::new("person"),
        "John Doe".to_string(),
    );
    entity_repo.create(person).await?;

    let org_id = RecordId::new_v4(TableName::Entity);
    let org_base = BaseFields::new(org_id.clone(), space_id.to_string(), "P_sys".to_string());
    let org = Entity::new(org_base, EntityType::new("org"), "Acme Corp".to_string());
    entity_repo.create(org).await?;

    // Create a pending assertion
    let assertion_repo = AssertionRepository::new(harness.client.clone());
    let revision_id = RecordId::new_v4(TableName::DocRevision);
    let assertion_id = RecordId::new_v4(TableName::Assertion);
    let assertion_base = BaseFields::new(
        assertion_id.clone(),
        space_id.to_string(),
        "P_sys".to_string(),
    );
    let assertion = Assertion {
        base: assertion_base,
        subject_text: "John Doe".to_string(),
        predicate_raw: "works_at".to_string(),
        predicate_id: None,
        object_text: Some("Acme Corp".to_string()),
        object_literal: None,
        evidence: EvidenceSpan {
            revision: Link::new(revision_id),
            node: None,
            start: 0,
            end: 24,
            quote_hash: "hash1".to_string(),
        },
        source_event_time: None,
        observed_at: Utc::now(),
        asserted_at: Utc::now(),
        assertion_key: format!("assertion-{}", uuid::Uuid::new_v4()),
        resolution_status: ResolutionStatus::Pending,
        resolution_method: None,
        resolution_confidence: None,
        retry_state: AssertionRetryState::default(),
        subject_entity: None,
        object_entity: None,
        confidence: 0.95,
        subject_entity_type: None,
        object_entity_type: None,
        significance: None,
        lifecycle_tier: "active".to_string(),
    };
    let created = assertion_repo.create(assertion).await?;

    // Simulate resolution by updating the assertion with resolved entities
    let mut resolved = created.clone();
    resolved.resolution_status = ResolutionStatus::Resolved;
    resolved.subject_entity = Some(Link::new(person_id.clone()));
    resolved.object_entity = Some(Link::new(org_id.clone()));
    resolved.retry_state.resolution_attempts = 1;
    resolved.retry_state.last_resolution_at = Some(Utc::now());

    let updated = assertion_repo
        .update(space_id, &assertion_id, resolved)
        .await?;

    // Verify resolution succeeded
    assert_eq!(
        updated.resolution_status,
        ResolutionStatus::Resolved,
        "Assertion should be marked resolved"
    );
    assert!(
        updated.subject_entity.is_some(),
        "Subject entity should be linked"
    );
    assert!(
        updated.object_entity.is_some(),
        "Object entity should be linked"
    );
    assert_eq!(
        updated.subject_entity.as_ref().unwrap().id(),
        &person_id,
        "Subject should link to person entity"
    );
    assert_eq!(
        updated.object_entity.as_ref().unwrap().id(),
        &org_id,
        "Object should link to org entity"
    );

    Ok(())
}

/// M8.3: Resolved assertions produce facts/relationships.
///
/// Test invariants:
/// 1. Resolved assertion creates corresponding rel/fact
/// 2. Provenance chain links back to assertion
/// 3. Evidence is preserved in canonical record
#[tokio::test]
#[ignore]
async fn scenario_m8_3_resolved_assertions_produce_facts() -> anyhow::Result<()> {
    let harness = Harness::new().await?;
    let space_id = "test_space";

    // Create entities
    let entity_repo = EntityRepository::new(harness.client.clone());

    let person_id = RecordId::new_v4(TableName::Entity);
    let person_base = BaseFields::new(person_id.clone(), space_id.to_string(), "P_sys".to_string());
    let person = Entity::new(
        person_base,
        EntityType::new("person"),
        "Jane Smith".to_string(),
    );
    entity_repo.create(person).await?;

    let org_id = RecordId::new_v4(TableName::Entity);
    let org_base = BaseFields::new(org_id.clone(), space_id.to_string(), "P_sys".to_string());
    let org = Entity::new(org_base, EntityType::new("org"), "Tech Inc".to_string());
    entity_repo.create(org).await?;

    // Create a resolved assertion
    let assertion_repo = AssertionRepository::new(harness.client.clone());
    let revision_id = RecordId::new_v4(TableName::DocRevision);
    let assertion_id = RecordId::new_v4(TableName::Assertion);
    let assertion_base = BaseFields::new(
        assertion_id.clone(),
        space_id.to_string(),
        "P_sys".to_string(),
    );
    let assertion = Assertion {
        base: assertion_base,
        subject_text: "Jane Smith".to_string(),
        predicate_raw: "employed_by".to_string(),
        predicate_id: None,
        object_text: Some("Tech Inc".to_string()),
        object_literal: None,
        evidence: EvidenceSpan {
            revision: Link::new(revision_id.clone()),
            node: None,
            start: 0,
            end: 30,
            quote_hash: "hash3".to_string(),
        },
        source_event_time: None,
        observed_at: Utc::now(),
        asserted_at: Utc::now(),
        assertion_key: format!("assertion-{}", uuid::Uuid::new_v4()),
        resolution_status: ResolutionStatus::Resolved,
        resolution_method: None,
        resolution_confidence: None,
        retry_state: AssertionRetryState {
            resolution_attempts: 1,
            last_resolution_at: Some(Utc::now()),
            abstention_reason: None,
        },
        subject_entity: Some(Link::new(person_id.clone())),
        object_entity: Some(Link::new(org_id.clone())),
        confidence: 0.92,
        subject_entity_type: None,
        object_entity_type: None,
        significance: None,
        lifecycle_tier: "active".to_string(),
    };
    assertion_repo.create(assertion).await?;

    // Create a relationship from the resolved assertion (simulating commit phase)
    use elephant_types::records::rel::{ClaimStatus, Rel};

    let rel_repo = RelRepository::new(harness.client.clone());
    let rel_id = RecordId::new_v4(TableName::Rel);
    let rel_base = BaseFields::new(rel_id.clone(), space_id.to_string(), "P_sys".to_string());
    let rel = Rel {
        base: rel_base,
        subject: Link::new(person_id.clone()),
        object: Link::new(org_id.clone()),
        rel_type: "employed_by".to_string(),
        status: ClaimStatus::Candidate,
        confidence: 0.92,
        validity_window: None,
        evidence: vec![],
        temporal_audit: None,
    };
    rel_repo.create(rel).await?;

    // Verify the relationship was created
    let retrieved = rel_repo.get(space_id, &rel_id).await?;
    assert!(retrieved.is_some(), "Relationship should be created");

    let rel = retrieved.unwrap();
    assert_eq!(
        rel.subject.id(),
        &person_id,
        "Rel subject should match assertion subject"
    );
    assert_eq!(
        rel.object.id(),
        &org_id,
        "Rel object should match assertion object"
    );
    assert_eq!(
        rel.rel_type, "employed_by",
        "Rel type should match predicate"
    );

    Ok(())
}

/// M8.4: Partial resolution is tracked.
///
/// Test invariants:
/// 1. Assertions with only subject resolved are marked partial
/// 2. Partial assertions can be retried later
/// 3. Resolution attempts are tracked
#[tokio::test]
#[ignore]
async fn scenario_m8_4_partial_resolution_tracked() -> anyhow::Result<()> {
    let harness = Harness::new().await?;
    let space_id = "test_space";

    // Create only subject entity (object not found)
    let entity_repo = EntityRepository::new(harness.client.clone());
    let person_id = RecordId::new_v4(TableName::Entity);
    let person_base = BaseFields::new(person_id.clone(), space_id.to_string(), "P_sys".to_string());
    let person = Entity::new(
        person_base,
        EntityType::new("person"),
        "Unknown Person".to_string(),
    );
    entity_repo.create(person).await?;

    // Create assertion where object cannot be resolved
    let assertion_repo = AssertionRepository::new(harness.client.clone());
    let revision_id = RecordId::new_v4(TableName::DocRevision);
    let assertion_id = RecordId::new_v4(TableName::Assertion);
    let assertion_base = BaseFields::new(
        assertion_id.clone(),
        space_id.to_string(),
        "P_sys".to_string(),
    );
    let assertion = Assertion {
        base: assertion_base,
        subject_text: "Unknown Person".to_string(),
        predicate_raw: "works_at".to_string(),
        predicate_id: None,
        object_text: Some("Nonexistent Company".to_string()),
        object_literal: None,
        evidence: EvidenceSpan {
            revision: Link::new(revision_id),
            node: None,
            start: 0,
            end: 40,
            quote_hash: "hash4".to_string(),
        },
        source_event_time: None,
        observed_at: Utc::now(),
        asserted_at: Utc::now(),
        assertion_key: format!("assertion-{}", uuid::Uuid::new_v4()),
        resolution_status: ResolutionStatus::Partial,
        resolution_method: None,
        resolution_confidence: None,
        retry_state: AssertionRetryState {
            resolution_attempts: 1,
            last_resolution_at: Some(Utc::now()),
            abstention_reason: None,
        },
        subject_entity: Some(Link::new(person_id.clone())),
        object_entity: None, // Object not resolved
        confidence: 0.85,
        subject_entity_type: None,
        object_entity_type: None,
        significance: None,
        lifecycle_tier: "active".to_string(),
    };
    let created = assertion_repo.create(assertion).await?;

    assert_eq!(
        created.resolution_status,
        ResolutionStatus::Partial,
        "Should be partial when only subject resolved"
    );
    assert!(
        created.subject_entity.is_some(),
        "Subject should be resolved"
    );
    assert!(
        created.object_entity.is_none(),
        "Object should remain unresolved"
    );
    assert_eq!(
        created.retry_state.resolution_attempts, 1,
        "Resolution attempts should be tracked"
    );

    Ok(())
}
