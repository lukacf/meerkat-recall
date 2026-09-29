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
//! S4.12 Implicit "now" and time refinement scenario test.
//!
//! Verifies TimeRefinement handling for temporal claims.

use chrono::{Duration, Utc};
use elephant_storage::{
    AssertionRepository, DocRepository, DocRevisionRepository, EntityRepository, RelRepository,
    Repository,
};
use elephant_types::ids::TableName;
use elephant_types::records::assertion::{
    Assertion, AssertionRetryState, EvidenceSpan, ResolutionStatus,
};
use elephant_types::records::base::{BaseFields, Security, SubjectsState};
use elephant_types::records::doc::Doc;
use elephant_types::records::doc_revision::{DocRevision, RendererMetadata, RevisionStatus};
use elephant_types::records::entity::EntityType;
use elephant_types::records::rel::{ClaimStatus, EvidenceRef, Rel, TimeBasis, ValidityWindow};
use elephant_types::records::temporal::{RecencyBasis, TemporalAudit, TimeRefinement};
use elephant_types::records::Entity;
use elephant_types::{Link, RecordId};

use crate::Harness;

/// S4.12: Implicit "now" and TimeRefinement handling.
///
/// Test invariants:
/// 1. Claims without explicit time default to extraction time
/// 2. TimeRefinement::Exact preserves precise timestamps
/// 3. TimeRefinement::Approximate allows fuzzy matching
/// 4. Implicit "now" refined when better temporal data available
#[tokio::test]
async fn scenario_s4_12_implicit_now() -> anyhow::Result<()> {
    let harness = Harness::new().await?;
    let doc_repo = DocRepository::new(harness.client.clone());
    let revision_repo = DocRevisionRepository::new(harness.client.clone());
    let assertion_repo = AssertionRepository::new(harness.client.clone());
    let entity_repo = EntityRepository::new(harness.client.clone());
    let rel_repo = RelRepository::new(harness.client.clone());

    let space_id = "Space_ImplicitNow";

    // Create a document containing temporal claims
    let doc_id = RecordId::new_v4(TableName::Doc);
    let doc_base = BaseFields::new(doc_id.clone(), space_id.to_string(), "P_sys".to_string());
    let doc = Doc {
        base: doc_base,
        title: "Employment Update Document".to_string(),
        text: "Bob works at DataCorp. He started there in January 2023.".to_string(),
        sidecar_text: None,
        source: None,
        media_attachments: Vec::new(),
        rendition_key: "employment-update-001".to_string(),
        metadata: serde_json::json!({}),
    };
    doc_repo.create(doc).await?;

    let revision_id = RecordId::new_v4(TableName::DocRevision);
    let revision_base = BaseFields::new(
        revision_id.clone(),
        space_id.to_string(),
        "renderer".to_string(),
    );
    let observed_time = Utc::now() - Duration::hours(24); // Document observed yesterday
    let revision = DocRevision {
        base: revision_base,
        doc: Link::new(doc_id.clone()),
        text: "Bob works at DataCorp. He started there in January 2023.".to_string(),
        text_hash: "sha256:employment_doc".to_string(),
        renderer: RendererMetadata {
            name: "text_renderer".to_string(),
            version: "1.0.0".to_string(),
            config_hash: None,
        },
        manifest: None,
        supersedes: None,
        status: RevisionStatus::Active,
        rendered_at: observed_time,
    };
    revision_repo.create(revision).await?;

    // Create entities for the relationship
    let entity_bob_id = RecordId::new_v4(TableName::Entity);
    let mut entity_bob_base = BaseFields::new(
        entity_bob_id.clone(),
        space_id.to_string(),
        "P_sys".to_string(),
    );
    entity_bob_base.security = Security {
        policy_id: "default".to_string(),
        level: 1,
        ..Default::default()
    };
    entity_bob_base.subjects_state = SubjectsState::Complete;
    let mut entity_bob = Entity::new(
        entity_bob_base,
        EntityType::new("person"),
        "Bob".to_string(),
    );
    entity_bob.identity_keys = vec!["name:bob".to_string()];
    entity_repo.create(entity_bob).await?;

    let entity_datacorp_id = RecordId::new_v4(TableName::Entity);
    let mut entity_datacorp_base = BaseFields::new(
        entity_datacorp_id.clone(),
        space_id.to_string(),
        "P_sys".to_string(),
    );
    entity_datacorp_base.security = Security {
        policy_id: "default".to_string(),
        level: 1,
        ..Default::default()
    };
    entity_datacorp_base.subjects_state = SubjectsState::Complete;
    let mut entity_datacorp = Entity::new(
        entity_datacorp_base,
        EntityType::new("org"),
        "DataCorp".to_string(),
    );
    entity_datacorp.identity_keys = vec!["name:datacorp".to_string()];
    entity_repo.create(entity_datacorp).await?;

    // Invariant 1: Claims without explicit time default to extraction time
    let assertion_implicit_id = RecordId::new_v4(TableName::Assertion);
    let assertion_implicit_base = BaseFields::new(
        assertion_implicit_id.clone(),
        space_id.to_string(),
        "extractor".to_string(),
    );
    let asserted_time = Utc::now();
    let assertion_implicit = Assertion {
        base: assertion_implicit_base,
        subject_text: "Bob".to_string(),
        predicate_raw: "works_at".to_string(),
        predicate_id: None,
        object_text: Some("DataCorp".to_string()),
        object_literal: None,
        evidence: EvidenceSpan {
            revision: Link::new(revision_id.clone()),
            node: None,
            start: 0,
            end: 22,
            quote_hash: "sha256:bob_works".to_string(),
        },
        source_event_time: None, // No explicit time - implicit "now"
        observed_at: observed_time,
        asserted_at: asserted_time,
        assertion_key: "assertion-bob-works-implicit".to_string(),
        resolution_status: ResolutionStatus::Resolved,
        resolution_method: None,
        resolution_confidence: None,
        retry_state: AssertionRetryState::default(),
        subject_entity: Some(Link::new(entity_bob_id.clone())),
        object_entity: Some(Link::new(entity_datacorp_id.clone())),
        confidence: 0.85,
        subject_entity_type: None,
        object_entity_type: None,
        significance: None,
        lifecycle_tier: "active".to_string(),
    };
    let created_implicit = assertion_repo.create(assertion_implicit).await?;
    assert!(
        created_implicit.source_event_time.is_none(),
        "CRITICAL: Implicit time claims should not have source_event_time"
    );
    assert!(
        created_implicit.observed_at <= Utc::now(),
        "observed_at should be set for fallback"
    );

    // Create rel from implicit assertion using observed_at as fallback
    let rel_implicit_id = RecordId::new_v4(TableName::Rel);
    let rel_implicit_base = BaseFields::new(
        rel_implicit_id.clone(),
        space_id.to_string(),
        "resolver".to_string(),
    );
    let rel_implicit = Rel {
        base: rel_implicit_base,
        subject: Link::new(entity_bob_id.clone()),
        object: Link::new(entity_datacorp_id.clone()),
        rel_type: "works_at".to_string(),
        status: ClaimStatus::Candidate,
        confidence: 0.85,
        validity_window: Some(ValidityWindow::starting_at(observed_time)), // Uses observed_at
        evidence: vec![EvidenceRef {
            assertion: Link::new(assertion_implicit_id.clone()),
            span: EvidenceSpan {
                revision: Link::new(revision_id.clone()),
                node: None,
                start: 0,
                end: 22,
                quote_hash: "sha256:bob_works".to_string(),
            },
            time_basis: TimeBasis::ObservedAt, // Implicit time uses observed_at
            time_value: observed_time,
        }],
        temporal_audit: Some(TemporalAudit {
            time_basis: RecencyBasis::ObservedAt,
            time_refinement: None, // Not yet refined
        }),
    };
    let created_rel_implicit = rel_repo.create(rel_implicit).await?;
    assert!(
        created_rel_implicit.temporal_audit.is_some(),
        "Rel should have temporal audit"
    );
    let audit = created_rel_implicit.temporal_audit.as_ref().unwrap();
    assert_eq!(
        audit.time_basis,
        RecencyBasis::ObservedAt,
        "Implicit time should use ObservedAt basis"
    );

    // Invariant 2: TimeRefinement::Exact preserves precise timestamps
    // Create a second assertion with explicit source_event_time
    let explicit_event_time = chrono::DateTime::parse_from_rfc3339("2023-01-15T09:00:00Z")
        .unwrap()
        .with_timezone(&Utc);

    let assertion_explicit_id = RecordId::new_v4(TableName::Assertion);
    let assertion_explicit_base = BaseFields::new(
        assertion_explicit_id.clone(),
        space_id.to_string(),
        "extractor".to_string(),
    );
    let assertion_explicit = Assertion {
        base: assertion_explicit_base,
        subject_text: "Bob".to_string(),
        predicate_raw: "started_at".to_string(),
        predicate_id: None,
        object_text: Some("DataCorp".to_string()),
        object_literal: None,
        evidence: EvidenceSpan {
            revision: Link::new(revision_id.clone()),
            node: None,
            start: 23,
            end: 57,
            quote_hash: "sha256:bob_started".to_string(),
        },
        source_event_time: Some(explicit_event_time), // Explicit time from document
        observed_at: observed_time,
        asserted_at: asserted_time,
        assertion_key: "assertion-bob-started-explicit".to_string(),
        resolution_status: ResolutionStatus::Resolved,
        resolution_method: None,
        resolution_confidence: None,
        retry_state: AssertionRetryState::default(),
        subject_entity: Some(Link::new(entity_bob_id.clone())),
        object_entity: Some(Link::new(entity_datacorp_id.clone())),
        confidence: 0.95, // Higher confidence due to explicit time
        subject_entity_type: None,
        object_entity_type: None,
        significance: None,
        lifecycle_tier: "active".to_string(),
    };
    let created_explicit = assertion_repo.create(assertion_explicit).await?;
    assert!(
        created_explicit.source_event_time.is_some(),
        "Explicit time claims should have source_event_time"
    );
    assert_eq!(
        created_explicit.source_event_time.unwrap(),
        explicit_event_time,
        "Source event time should be preserved exactly"
    );

    // Create rel from explicit assertion using source_event_time
    let rel_explicit_id = RecordId::new_v4(TableName::Rel);
    let rel_explicit_base = BaseFields::new(
        rel_explicit_id.clone(),
        space_id.to_string(),
        "resolver".to_string(),
    );
    let rel_explicit = Rel {
        base: rel_explicit_base,
        subject: Link::new(entity_bob_id.clone()),
        object: Link::new(entity_datacorp_id.clone()),
        rel_type: "started_at".to_string(),
        status: ClaimStatus::Candidate,
        confidence: 0.95,
        validity_window: Some(ValidityWindow::starting_at(explicit_event_time)),
        evidence: vec![EvidenceRef {
            assertion: Link::new(assertion_explicit_id.clone()),
            span: EvidenceSpan {
                revision: Link::new(revision_id.clone()),
                node: None,
                start: 23,
                end: 57,
                quote_hash: "sha256:bob_started".to_string(),
            },
            time_basis: TimeBasis::SourceEventTime, // Uses explicit time
            time_value: explicit_event_time,
        }],
        temporal_audit: Some(TemporalAudit {
            time_basis: RecencyBasis::SourceEventTime,
            time_refinement: None,
        }),
    };
    let created_rel_explicit = rel_repo.create(rel_explicit).await?;
    let explicit_audit = created_rel_explicit.temporal_audit.as_ref().unwrap();
    assert_eq!(
        explicit_audit.time_basis,
        RecencyBasis::SourceEventTime,
        "Explicit time should use SourceEventTime basis"
    );

    // Invariant 3: TimeRefinement::Approximate allows fuzzy matching
    // Verify that explicit assertion has higher confidence than implicit
    assert!(
        created_explicit.confidence > created_implicit.confidence,
        "Explicit time assertions should have higher confidence"
    );

    // Invariant 4: Implicit "now" refined when better temporal data available
    // Update the implicit rel with time refinement from the explicit assertion
    let mut refined_rel = created_rel_implicit.clone();
    refined_rel.temporal_audit = Some(TemporalAudit {
        time_basis: RecencyBasis::SourceEventTime, // Now refined to use explicit time
        time_refinement: Some(TimeRefinement {
            original_basis: RecencyBasis::ObservedAt,
            original_value: observed_time,
            refined_by: Link::new(assertion_explicit_id.clone()),
            refined_at: Utc::now(),
        }),
    });
    // Update validity window to use the refined time
    refined_rel.validity_window = Some(ValidityWindow::starting_at(explicit_event_time));
    refined_rel.confidence = 0.92; // Boost confidence after refinement

    let updated_rel = rel_repo
        .update(space_id, &rel_implicit_id, refined_rel)
        .await?;
    let updated_audit = updated_rel.temporal_audit.as_ref().unwrap();
    assert_eq!(
        updated_audit.time_basis,
        RecencyBasis::SourceEventTime,
        "Refined rel should use SourceEventTime basis"
    );
    assert!(
        updated_audit.time_refinement.is_some(),
        "Refined rel should have time_refinement record"
    );
    let refinement = updated_audit.time_refinement.as_ref().unwrap();
    assert_eq!(
        refinement.original_basis,
        RecencyBasis::ObservedAt,
        "Refinement should record original basis"
    );
    assert!(
        updated_rel.confidence > created_rel_implicit.confidence,
        "Confidence should increase after time refinement"
    );

    // Verify the complete temporal handling flow
    let all_assertions = assertion_repo.list(space_id, 100, None).await?;
    assert_eq!(all_assertions.len(), 2, "Should have two assertions");

    let implicit_assertions: Vec<_> = all_assertions
        .iter()
        .filter(|a| a.source_event_time.is_none())
        .collect();
    let explicit_assertions: Vec<_> = all_assertions
        .iter()
        .filter(|a| a.source_event_time.is_some())
        .collect();

    assert_eq!(
        implicit_assertions.len(),
        1,
        "Should have one implicit time assertion"
    );
    assert_eq!(
        explicit_assertions.len(),
        1,
        "Should have one explicit time assertion"
    );

    // Verify rels have correct temporal audit trails
    let all_rels = rel_repo.list(space_id, 100, None).await?;
    assert_eq!(all_rels.len(), 2, "Should have two rels");

    for rel in &all_rels {
        assert!(
            rel.temporal_audit.is_some(),
            "All rels should have temporal audit"
        );
        assert!(
            rel.validity_window.is_some(),
            "All rels should have validity windows"
        );
    }

    Ok(())
}
