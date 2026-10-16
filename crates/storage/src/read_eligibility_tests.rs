//! Content-proof and retained-read oracles. Synthetic proof values below exercise
//! a pure comparison; they do not insert publication heads or certify a source.
#![allow(clippy::expect_used, clippy::unwrap_used, clippy::float_cmp)]

use super::*;
use chrono::Utc;
use elephant_types::records::{AssertionRetryState, EntityType, ResolutionStatus, SourceType};
use elephant_types::{Link, RecordId};

const SPACE: &str = "read-proof";

fn base(table: TableName) -> BaseFields {
    BaseFields::new(RecordId::new_v4(table), SPACE.into(), "oracle".into())
}

fn assertion() -> Assertion {
    Assertion {
        base: base(TableName::Assertion),
        subject_text: "Ada".into(),
        predicate_raw: "role".into(),
        predicate_id: Some("role".into()),
        object_text: Some("researcher".into()),
        object_literal: None,
        evidence: EvidenceSpan {
            revision: Link::new(RecordId::new_v4(TableName::DocRevision)),
            node: None,
            start: 0,
            end: 0,
            quote_hash: String::new(),
        },
        source_event_time: None,
        observed_at: Utc::now(),
        asserted_at: Utc::now(),
        assertion_key: uuid::Uuid::new_v4().to_string(),
        resolution_status: ResolutionStatus::Pending,
        resolution_method: None,
        resolution_confidence: None,
        retry_state: AssertionRetryState::default(),
        subject_entity: None,
        object_entity: None,
        confidence: 0.9,
        subject_entity_type: None,
        object_entity_type: None,
        significance: None,
        lifecycle_tier: "active".into(),
    }
}

fn proof<T: Serialize>(base: &BaseFields, record: &T, independent: bool) -> SourceEffectSupport {
    SourceEffectSupport {
        last_commit_id: None,
        space_id: SPACE.into(),
        source_id: RecordId::new_v4(TableName::Source),
        source_revision: "pure-comparison-only".into(),
        record_id: base.id.clone(),
        historical_unknown: None,
        independent,
        record_fingerprint: source_support_fingerprint(record).unwrap(),
    }
}

#[test]
fn an_assertion_id_and_unchanged_quote_do_not_certify_changed_claim_content() {
    let original = assertion();
    let support = vec![proof(&original.base, &original, false)];
    assert!(support_matches_record(&original.base, &original, &support).unwrap());
    let mut changed = original.clone();
    changed.object_text = Some("invented different role".into());
    assert_eq!(changed.evidence, original.evidence);
    assert!(!support_matches_record(&changed.base, &changed, &support).unwrap());
    changed = original.clone();
    changed.predicate_raw = "owns".into();
    assert!(!support_matches_record(&changed.base, &changed, &support).unwrap());
    changed = original.clone();
    changed.object_literal = Some(serde_json::json!({"private":"payload"}));
    assert!(!support_matches_record(&changed.base, &changed, &support).unwrap());
    changed = original;
    changed.evidence.revision = Link::new(RecordId::new_v4(TableName::DocRevision));
    assert!(!support_matches_record(&changed.base, &changed, &support).unwrap());
}

#[test]
fn content_proof_covers_security_subjects_and_source_pointers() {
    let original = assertion();
    let support = vec![proof(&original.base, &original, true)];
    let mut changed = original.clone();
    changed.base.security.labels.push("private".into());
    assert!(!support_matches_record(&changed.base, &changed, &support).unwrap());
    changed = original.clone();
    changed
        .base
        .subjects
        .push(Link::new(RecordId::new_v4(TableName::Entity)));
    assert!(!support_matches_record(&changed.base, &changed, &support).unwrap());
    changed = original;
    changed
        .base
        .provenance
        .sources
        .push(RecordId::new_v4(TableName::Source));
    assert!(!support_matches_record(&changed.base, &changed, &support).unwrap());
}

#[test]
fn independent_flag_cannot_authorize_a_changed_or_equal_named_entity() {
    let original = Entity::new(
        base(TableName::Entity),
        EntityType::new("person"),
        "Ada".into(),
    );
    let support = vec![proof(&original.base, &original, true)];
    let mut changed = original.clone();
    changed.aliases.push("private source alias".into());
    assert!(!support_matches_record(&changed.base, &changed, &support).unwrap());
    changed = original;
    changed.base.id = RecordId::new_v4(TableName::Entity);
    assert!(!support_matches_record(&changed.base, &changed, &support).unwrap());
}

#[test]
fn one_matching_support_cannot_mask_another_stale_payload_proof() {
    let original = assertion();
    let current = proof(&original.base, &original, true);
    let mut stale = proof(&original.base, &original, false);
    stale.record_fingerprint = "old-proof".into();
    assert!(!support_matches_record(&original.base, &original, &[current, stale]).unwrap());
}

#[test]
fn only_updated_at_is_ignored_by_record_content_proof() {
    let original = assertion();
    let support = vec![proof(&original.base, &original, false)];
    let mut changed = original.clone();
    changed.base.updated_at += chrono::Duration::seconds(1);
    assert!(support_matches_record(&changed.base, &changed, &support).unwrap());
    changed = original;
    changed.base.created_at += chrono::Duration::seconds(1);
    assert!(!support_matches_record(&changed.base, &changed, &support).unwrap());
}

async fn client(embedded: bool) -> StorageClient {
    let client = if embedded {
        StorageClient::new_embedded().await
    } else {
        StorageClient::new_memory().await
    }
    .unwrap();
    if !embedded {
        client
            .query_checked(include_str!(
                "../../migrations/sql/031_source_publication.surql"
            ))
            .await
            .unwrap();
        client
            .query_checked(include_str!(
                "../../migrations/sql/046_source_support_index_fence.surql"
            ))
            .await
            .unwrap();
    }
    client
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
async fn intrinsic_records_remain_readable_without_manufactured_source_proof() {
    for embedded in [true, false] {
        let client = client(embedded).await;
        let original = crate::EntityRepository::new(client.clone())
            .create(Entity::new(
                base(TableName::Entity),
                EntityType::new("person"),
                "independently authored".into(),
            ))
            .await
            .unwrap();
        let reader = ReadEligibility::new(client.clone());
        assert_eq!(
            reader.project(original.clone(), &|_| true).await.unwrap(),
            Some(original)
        );
        let original = AssertionRepository::new(client.clone())
            .create(assertion())
            .await
            .unwrap();
        assert_eq!(
            reader.project(original.clone(), &|_| true).await.unwrap(),
            Some(original)
        );
        let mut deleted = assertion();
        deleted.base.soft_delete();
        assert!(reader.project(deleted, &|_| true).await.unwrap().is_none());
    }
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
async fn source_removal_never_preserves_an_unscrubbed_shared_alias() {
    for embedded in [true, false] {
        let client = client(embedded).await;
        let sources = SourceRepository::new(client.clone());
        let first = sources
            .create(Source::new(
                base(TableName::Source),
                SourceType::Manual,
                "legacy-a".into(),
            ))
            .await
            .unwrap();
        let second = sources
            .create(Source::new(
                base(TableName::Source),
                SourceType::Manual,
                "legacy-b".into(),
            ))
            .await
            .unwrap();
        let mut entity = Entity::new(
            base(TableName::Entity),
            EntityType::new("person"),
            "shared".into(),
        );
        entity.base.provenance.sources = vec![first.base.id, second.base.id.clone()];
        entity.aliases = vec!["alias contributed by the second source".into()];
        let entity = crate::EntityRepository::new(client.clone())
            .create(entity)
            .await
            .unwrap();
        assert!(ReadEligibility::new(client.clone())
            .project(entity.clone(), &|_| true)
            .await
            .unwrap()
            .is_some());
        sources.delete(SPACE, &second.base.id).await.unwrap();
        assert!(ReadEligibility::new(client)
            .project(entity, &|_| true)
            .await
            .unwrap()
            .is_none());
        // Canonical lifecycle reconciliation owns restoring a safe independent
        // baseline. This read oracle never pretends it performed that erasure.
    }
}

#[tokio::test]
async fn event_projection_cannot_keep_text_after_dropping_a_source_reference() {
    let client = client(true).await;
    let source = SourceRepository::new(client.clone())
        .create(Source::new(
            base(TableName::Source),
            SourceType::Manual,
            "retained".into(),
        ))
        .await
        .unwrap();
    let mut event = Event::new(
        base(TableName::Event),
        "text from both inputs",
        vec![],
        vec![
            Link::new(source.base.id.clone()),
            Link::new(RecordId::new_v4(TableName::Source)),
        ],
    )
    .unwrap();
    event.base.provenance.sources = vec![source.base.id];
    let event = crate::EventRepository::new(client.clone())
        .create(event)
        .await
        .unwrap();
    assert!(ReadEligibility::new(client)
        .project(event, &|_| true)
        .await
        .unwrap()
        .is_none());
}

#[test]
fn coalesced_bundle_allows_only_the_exact_accepted_anchor_and_window() {
    use crate::source_publication_manifest::ExpectedSourceBundle;
    let revision = RecordId::new_v4(TableName::DocRevision);
    let anchor = RecordId::new_v4(TableName::DocNode);
    let later = RecordId::new_v4(TableName::DocNode);
    let bundle = ExpectedSourceBundle {
        artifact_id: RecordId::new_v4(TableName::WorkArtifact),
        node_id: anchor.clone(),
        node_ids: vec![anchor.clone(), later.clone()],
        revision_id: revision.clone(),
        span_start: 10,
        span_end: 90,
    };
    // The quote lies after the first node's physical 10..30 span. Only the
    // persisted bundle window, not the whole revision, grants this range.
    let span = EvidenceSpan {
        revision: Link::new(revision.clone()),
        node: Some(Link::new(anchor.clone())),
        start: 50,
        end: 70,
        quote_hash: "range comparison only".into(),
    };
    assert!(accepted_bundle_contains(&bundle, &revision, &anchor, &span));
    assert!(!accepted_bundle_contains(&bundle, &revision, &later, &span));
    assert!(!accepted_bundle_contains(
        &bundle,
        &RecordId::new_v4(TableName::DocRevision),
        &anchor,
        &span
    ));
    for (start, end) in [(9, 20), (80, 91), (50, 50), (70, 50)] {
        let mut invalid = span.clone();
        invalid.start = start;
        invalid.end = end;
        assert!(!accepted_bundle_contains(
            &bundle, &revision, &anchor, &invalid
        ));
    }
    let mut malformed = bundle;
    malformed.node_ids.swap(0, 1);
    assert!(!accepted_bundle_contains(
        &malformed, &revision, &anchor, &span
    ));
}

#[tokio::test]
async fn absent_manifest_keeps_physical_node_and_utf8_bounds() {
    use elephant_types::records::doc_node::TextSpan;
    use elephant_types::records::doc_revision::RendererMetadata;
    use elephant_types::records::NodeRole;
    let client = client(true).await;
    let revision = DocRevision {
        base: base(TableName::DocRevision),
        doc: Link::new(RecordId::new_v4(TableName::Doc)),
        text: "first node; later café".into(),
        text_hash: "range comparison only".into(),
        renderer: RendererMetadata {
            name: "range-oracle".into(),
            version: "1".into(),
            config_hash: None,
        },
        manifest: None,
        supersedes: None,
        status: RevisionStatus::Active,
        rendered_at: Utc::now(),
    };
    let node = DocNode {
        base: base(TableName::DocNode),
        revision: Link::new(revision.base.id.clone()),
        role: NodeRole::Paragraph,
        span: TextSpan::new(0, 10),
        node_key: "first".into(),
        text: None,
        parent: None,
        position: 0,
        depth: 0,
        embedding_model: None,
        collection: None,
    };
    let mut span = EvidenceSpan {
        revision: Link::new(revision.base.id.clone()),
        node: Some(Link::new(node.base.id.clone())),
        start: 0,
        end: 5,
        quote_hash: "range comparison only".into(),
    };
    assert!(
        evidence_within_accepted_window(&client, SPACE, &revision, &node, &span)
            .await
            .unwrap()
    );
    span.start = 12;
    span.end = 17;
    assert!(
        !evidence_within_accepted_window(&client, SPACE, &revision, &node, &span)
            .await
            .unwrap()
    );
    span.start = 20;
    span.end = 21; // Ends inside the final UTF-8 character.
    assert!(
        !evidence_within_accepted_window(&client, SPACE, &revision, &node, &span)
            .await
            .unwrap()
    );
}

#[test]
fn wiki_compiler_descriptors_are_explicit_and_never_record_authority() {
    use elephant_types::records::KnowledgeDependency;
    for (record_type, role, record_id) in [
        ("model", "article_writer", "model:approved-writer"),
        ("model", "article_verifier", "model:approved-verifier"),
        ("prompt", "article_writer_prompt", "prompt:writer-v1"),
        ("prompt", "article_verifier_prompt", "prompt:verifier-v1"),
    ] {
        let descriptor = KnowledgeDependency {
            record_type: record_type.into(),
            role: role.into(),
            record_id: record_id.into(),
            observed_updated_at: None,
        };
        assert!(wiki_descriptor(&descriptor));
        let mut changed = descriptor.clone();
        changed.record_type = "entity".into();
        assert!(!wiki_descriptor(&changed));
        changed = descriptor.clone();
        changed.role = "evidence".into();
        assert!(!wiki_descriptor(&changed));
        changed = descriptor.clone();
        changed.observed_updated_at = Some(Utc::now());
        assert!(!wiki_descriptor(&changed));
        changed = descriptor;
        changed.record_id = format!("{record_type}:");
        assert!(!wiki_descriptor(&changed));
    }
}

#[test]
fn wiki_context_walk_rejects_cycles_and_excess_depth() {
    let id = RecordId::new_v4(TableName::KnowledgeViewRevision);
    assert!(!wiki_path_allows(std::slice::from_ref(&id), &id));
    let path: Vec<_> = (0..32)
        .map(|_| RecordId::new_v4(TableName::KnowledgeViewRevision))
        .collect();
    assert!(!wiki_path_allows(&path, &id));
    assert!(wiki_path_allows(&path[..31], &id));
}

fn wiki_pair(
    root: &Entity,
) -> (
    elephant_types::records::KnowledgeView,
    elephant_types::records::KnowledgeViewRevision,
) {
    use elephant_types::records::knowledge_view::*;
    use elephant_types::records::HasSecurity;
    let mut view_base = root.base.clone();
    view_base.id = RecordId::new_v4(TableName::KnowledgeView);
    view_base.subjects = root.subjects();
    view_base.subjects_state = root.subjects_state();
    let mut revision_base = view_base.clone();
    revision_base.id = RecordId::new_v4(TableName::KnowledgeViewRevision);
    let view = KnowledgeView {
        key: view_base.id.to_string(),
        base: view_base.clone(),
        kind: KnowledgeViewKind::Entity,
        surface: KnowledgeViewSurface::Inspector,
        publication_status: KnowledgeViewPublicationStatus::Published,
        root_record: root.base.id.to_string(),
        title: "context".into(),
        slug: "context".into(),
        security_fingerprint: "source-less-oracle".into(),
        current_revision: Some(revision_base.id.to_string()),
        revision_count: 1,
        state: ProjectionState::Current,
        state_reason: None,
        published_at: Some(Utc::now()),
    };
    let revision = KnowledgeViewRevision {
        base: revision_base,
        view_id: view_base.id.to_string(),
        revision: 1,
        kind: KnowledgeViewKind::Entity,
        root_record: root.base.id.to_string(),
        blocks: vec![],
        rendered_markdown: String::new(),
        citations: vec![],
        dependencies: vec![KnowledgeDependency {
            record_id: root.base.id.to_string(),
            record_type: "entity".into(),
            role: "root".into(),
            observed_updated_at: Some(root.base.updated_at),
        }],
        freshness: ProjectionFreshness {
            state: ProjectionState::Current,
            minimum_claim_score: None,
            stale_truth_slots: 0,
            unresolved_conflicts: 0,
            unsupported_claims: 0,
            expired_claims: 0,
            selection_truncated: false,
            valid_until: None,
        },
        validation: ValidationReceipt {
            citations_resolve: true,
            factual_blocks_cited: true,
            statements_supported: true,
            prose_is_structured: true,
            factual_block_count: 0,
            citation_count: 0,
            statement_count: 0,
            warnings: vec![],
        },
        compiler: CompilerReceipt {
            compiler: "read-context-oracle".into(),
            version: 1,
            dependency_hash: "source-less-oracle".into(),
            policy_fingerprint: "source-less-oracle".into(),
        },
        compiled_at: Utc::now(),
        supersedes: None,
    };
    (view, revision)
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
async fn wiki_context_dependencies_load_real_rows_and_reject_changed_inputs() {
    use elephant_types::records::KnowledgeDependency;
    let client = client(true).await;
    crate::SpaceConfigRepository::new(client.clone())
        .create(elephant_types::records::SpaceConfig {
            base: base(TableName::SpaceConfig),
            embedding_provider: "unconfigured".into(),
            embedding_model: "unconfigured".into(),
            embedding_dims: 1,
            processing: None,
            purpose: None,
            skill: None,
        })
        .await
        .unwrap();
    let entities = crate::EntityRepository::new(client.clone());
    let root = entities
        .create(Entity::new(
            base(TableName::Entity),
            EntityType::new("person"),
            "Ada".into(),
        ))
        .await
        .unwrap();
    let (view, context) = wiki_pair(&root);
    let (view, context) = crate::KnowledgeViewRepository::new(client.clone())
        .publish(view, context)
        .await
        .unwrap();
    let (_, mut article) = wiki_pair(&root);
    article.dependencies.extend([
        KnowledgeDependency {
            record_id: view.base.id.to_string(),
            record_type: "knowledge_view".into(),
            role: "editorial_context".into(),
            observed_updated_at: Some(view.base.updated_at),
        },
        KnowledgeDependency {
            record_id: context.base.id.to_string(),
            record_type: "knowledge_view_revision".into(),
            role: "editorial_context_revision".into(),
            observed_updated_at: Some(context.base.updated_at),
        },
        KnowledgeDependency {
            record_id: "model:fixture".into(),
            record_type: "model".into(),
            role: "article_writer".into(),
            observed_updated_at: None,
        },
    ]);
    assert!(ReadEligibility::new(client.clone())
        .project(article.clone(), &|_| true)
        .await
        .unwrap()
        .is_some());
    let mut changed = root.clone();
    changed.display_name = "Changed after compilation".into();
    changed.base.updated_at += chrono::Duration::seconds(1);
    entities
        .update(SPACE, &root.base.id, changed)
        .await
        .unwrap();
    assert!(ReadEligibility::new(client)
        .project(article, &|_| true)
        .await
        .unwrap()
        .is_none());
}

#[test]
fn wiki_source_dependency_distinguishes_accepted_content_from_lifecycle_adornments() {
    use elephant_types::records::source::SourceLifecycleStatus;
    let accepted = Source::new(
        base(TableName::Source),
        SourceType::Manual,
        "accepted".into(),
    );
    let mut live = accepted.clone();
    live.base.updated_at += chrono::Duration::seconds(10);
    live.lifecycle_status = SourceLifecycleStatus::GraphReady;
    assert!(wiki_source_dependency_matches(
        Some(live.base.updated_at),
        &live,
        &accepted
    ));
    // A previously compiled accepted snapshot remains valid while new input is
    // pending. Newly observed raw content cannot borrow the older publication.
    live.metadata = serde_json::json!({"private":"new unpublished input"});
    assert!(wiki_source_dependency_matches(
        Some(accepted.base.updated_at),
        &live,
        &accepted
    ));
    assert!(!wiki_source_dependency_matches(
        Some(live.base.updated_at),
        &live,
        &accepted
    ));
    assert!(!wiki_source_dependency_matches(None, &live, &accepted));
}

#[test]
fn wiki_source_citations_keep_the_captured_published_snapshot() {
    use elephant_types::records::{CitationKind, KnowledgeCitation, KnowledgeDependency};

    let accepted = Source::new(
        base(TableName::Source),
        SourceType::Manual,
        "accepted content".into(),
    );
    let citation = KnowledgeCitation {
        id: "original-source-citation".into(),
        kind: CitationKind::GraphRecord,
        record_id: accepted.base.id.to_string(),
        assertion_id: None,
        source_id: Some(accepted.base.id.to_string()),
        evidence_span: None,
    };
    let captured = KnowledgeDependency {
        record_id: accepted.base.id.to_string(),
        record_type: "source".into(),
        role: "root".into(),
        observed_updated_at: Some(accepted.base.updated_at),
    };
    let dependency = wiki_citation_dependency(
        &citation,
        &accepted.base.id,
        std::slice::from_ref(&captured),
    );

    // Canonical metadata completion adds fields to the live Source without
    // rewriting the immutable accepted snapshot used by this source view.
    let mut live = accepted.clone();
    live.base.updated_at += chrono::Duration::seconds(10);
    live.lifecycle_status = elephant_types::records::source::SourceLifecycleStatus::GraphReady;
    live.current_ingest_mode = Some(elephant_types::records::ingest::IngestMode::Graph);
    live.metadata = serde_json::json!({"title":"Original normalized title"});
    assert!(!wiki_source_dependency_matches(None, &live, &accepted));
    assert!(wiki_source_dependency_matches(
        captured.observed_updated_at,
        &live,
        &accepted
    ));
    assert!(wiki_source_dependency_matches(
        dependency.observed_updated_at,
        &live,
        &accepted
    ));

    // The same captured publication stays readable while a successor is pending.
    // Its new desired bytes cannot become the cited source's observation.
    live.base.updated_at += chrono::Duration::seconds(10);
    live.hash = "new pending content".into();
    live.metadata = serde_json::json!({"private":"new unpublished input"});
    assert!(wiki_source_dependency_matches(
        dependency.observed_updated_at,
        &live,
        &accepted
    ));
    assert!(!wiki_source_dependency_matches(
        Some(live.base.updated_at),
        &live,
        &accepted
    ));

    let mut replacement = accepted;
    replacement.base.updated_at += chrono::Duration::seconds(30);
    replacement.hash = "different completed content".into();
    assert!(!wiki_source_dependency_matches(
        dependency.observed_updated_at,
        &replacement,
        &replacement
    ));
}

#[test]
fn wiki_source_citations_refuse_missing_conflicting_or_foreign_observations() {
    use elephant_types::records::{CitationKind, KnowledgeCitation, KnowledgeDependency};

    let accepted = Source::new(
        base(TableName::Source),
        SourceType::Manual,
        "accepted".into(),
    );
    let citation = KnowledgeCitation {
        id: "unbound-source-citation".into(),
        kind: CitationKind::GraphRecord,
        record_id: accepted.base.id.to_string(),
        assertion_id: None,
        source_id: Some(accepted.base.id.to_string()),
        evidence_span: None,
    };
    let captured = KnowledgeDependency {
        record_id: accepted.base.id.to_string(),
        record_type: "source".into(),
        role: "root".into(),
        observed_updated_at: Some(accepted.base.updated_at),
    };
    let mut live = accepted.clone();
    live.base.updated_at += chrono::Duration::seconds(10);
    live.metadata = serde_json::json!({"private":"pending source metadata"});
    let mut missing_time = captured.clone();
    missing_time.observed_updated_at = None;
    let mut conflicting = captured.clone();
    conflicting.observed_updated_at = Some(live.base.updated_at);
    let mut foreign = captured.clone();
    foreign.record_id = RecordId::new_v4(TableName::Source).to_string();
    let mut wrong_type = captured.clone();
    wrong_type.record_type = "doc".into();
    for observations in [
        vec![],
        vec![missing_time.clone()],
        vec![foreign],
        vec![wrong_type],
        vec![captured.clone(), conflicting.clone()],
        vec![conflicting, captured.clone()],
        vec![captured, missing_time],
    ] {
        let dependency = wiki_citation_dependency(&citation, &accepted.base.id, &observations);
        assert!(dependency.observed_updated_at.is_none());
        assert!(!wiki_source_dependency_matches(
            dependency.observed_updated_at,
            &live,
            &accepted
        ));
    }

    // Other record kinds retain their original unbound citation check; this
    // Source-specific snapshot binding cannot loosen their current-row checks.
    let doc_id = RecordId::new_v4(TableName::Doc);
    let mut doc_citation = citation;
    doc_citation.record_id = doc_id.to_string();
    doc_citation.source_id = None;
    let doc_observation = KnowledgeDependency {
        record_id: doc_id.to_string(),
        record_type: "doc".into(),
        role: "source_document".into(),
        observed_updated_at: Some(accepted.base.updated_at),
    };
    assert!(
        wiki_citation_dependency(&doc_citation, &doc_id, &[doc_observation])
            .observed_updated_at
            .is_none()
    );
}

#[test]
fn shared_writer_verifier_descriptors_require_every_role_to_be_known() {
    let mut descriptor = elephant_types::records::KnowledgeDependency {
        record_id: "model:shared".into(),
        record_type: "model".into(),
        role: "article_verifier+article_writer".into(),
        observed_updated_at: None,
    };
    assert!(wiki_descriptor(&descriptor));
    for role in [
        "article_writer+evidence",
        "article_writer+",
        "",
        "article_writer_prompt",
    ] {
        descriptor.role = role.into();
        assert!(!wiki_descriptor(&descriptor));
    }
}

#[test]
fn selector_descriptors_require_exact_table_role_and_absent_snapshot() {
    use elephant_types::records::KnowledgeDependency;
    for table in ["entity", "attribute", "rel"] {
        let descriptor = KnowledgeDependency {
            record_type: "selector_trigger".into(),
            record_id: format!("trigger:table:{table}"),
            role: "potential_membership_change".into(),
            observed_updated_at: None,
        };
        assert!(wiki_descriptor(&descriptor));
        for id in [
            "trigger:table:source",
            "trigger:table:entity:extra",
            "trigger:table:Entity",
            "trigger:table:",
            "entity:123",
        ] {
            let mut changed = descriptor.clone();
            changed.record_id = id.into();
            assert!(!wiki_descriptor(&changed));
        }
        for role in ["evidence", "potential_membership_change+evidence", ""] {
            let mut changed = descriptor.clone();
            changed.role = role.into();
            assert!(!wiki_descriptor(&changed));
        }
        let mut changed = descriptor.clone();
        changed.observed_updated_at = Some(Utc::now());
        assert!(!wiki_descriptor(&changed));
        changed = descriptor;
        changed.record_type = table.into();
        assert!(!wiki_descriptor(&changed));
    }
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
#[expect(
    clippy::too_many_lines,
    reason = "Keep the complete ordered conformance scenario and its failure assertions together."
)]
async fn wiki_typed_dependency_root_preserves_parent_subject_requirements() {
    use elephant_types::records::base::SubjectsState;
    for embedded in [true, false] {
        let client = client(embedded).await;
        let entities = crate::EntityRepository::new(client.clone());
        let mut root = entities
            .create(Entity::new(
                base(TableName::Entity),
                EntityType::new("person"),
                "Ada".into(),
            ))
            .await
            .unwrap();
        assert_eq!(root.base.subjects_state, SubjectsState::Unknown);
        let object = entities
            .create(Entity::new(
                base(TableName::Entity),
                EntityType::new("organization"),
                "Research".into(),
            ))
            .await
            .unwrap();
        let rels = crate::RelRepository::new(client.clone());
        let mut rel = rels
            .create(Rel {
                base: base(TableName::Rel),
                subject: Link::new(root.base.id.clone()),
                object: Link::new(object.base.id.clone()),
                rel_type: "works_at".into(),
                status: elephant_types::records::ClaimStatus::Approved,
                confidence: 1.0,
                validity_window: None,
                evidence: vec![],
                temporal_audit: None,
            })
            .await
            .unwrap();
        assert_eq!(rel.base.subjects_state, SubjectsState::Unknown);
        let (_, mut revision) = wiki_pair(&root);
        revision
            .dependencies
            .push(elephant_types::records::KnowledgeDependency {
                record_id: rel.base.id.to_string(),
                record_type: "rel".into(),
                role: "relationship".into(),
                observed_updated_at: Some(rel.base.updated_at),
            });
        revision.base.subjects = vec![
            Link::new(root.base.id.clone()),
            Link::new(object.base.id.clone()),
        ];
        revision.base.subjects_state = SubjectsState::Complete;
        let output = revision.base.clone();
        let allowed = |input: &BaseFields| {
            input.subjects_state == SubjectsState::Complete
                && input
                    .subjects
                    .iter()
                    .all(|subject| output.subjects.contains(subject))
                && input.security.level <= output.security.level
                && input.security.policy_id == output.security.policy_id
        };
        assert!(ReadEligibility::new(client.clone())
            .project(revision.clone(), &allowed)
            .await
            .unwrap()
            .is_some());

        let original_rel = rel.clone();
        rel.base.security.policy_id = "different-policy".into();
        rel = rels.update(SPACE, &rel.base.id, rel.clone()).await.unwrap();
        revision.dependencies[1].observed_updated_at = Some(rel.base.updated_at);
        assert!(ReadEligibility::new(client.clone())
            .project(revision.clone(), &allowed)
            .await
            .unwrap()
            .is_none());
        rel = rels
            .update(SPACE, &rel.base.id, original_rel)
            .await
            .unwrap();
        revision.dependencies[1].observed_updated_at = Some(rel.base.updated_at);

        // A legacy actual source has no generated preview head. Its Unknown
        // scope must not be replaced by the entity's implicit Complete scope.
        let source = crate::SourceRepository::new(client.clone())
            .create(Source::new(
                base(TableName::Source),
                SourceType::Manual,
                "typed-root-parent".into(),
            ))
            .await
            .unwrap();
        root.base.provenance.sources = vec![source.base.id.clone()];
        root = entities
            .update(SPACE, &root.base.id, root.clone())
            .await
            .unwrap();
        revision.dependencies[0].observed_updated_at = Some(root.base.updated_at);
        let allowed = |input: &BaseFields| input.subjects_state == SubjectsState::Complete;
        let reader = ReadEligibility::new(client.clone());
        assert!(reader
            .project(revision.clone(), &allowed)
            .await
            .unwrap()
            .is_none());
        assert!(reader
            .project(revision.clone(), &|_| true)
            .await
            .unwrap()
            .is_some());

        // Restrictive Partial state stays Partial even though Entity supplies
        // its own implicit identity subject.
        root.base.provenance.sources.clear();
        root.base.subjects_state = SubjectsState::Partial;
        root = entities
            .update(SPACE, &root.base.id, root.clone())
            .await
            .unwrap();
        revision.dependencies[0].observed_updated_at = Some(root.base.updated_at);
        assert!(ReadEligibility::new(client)
            .project(revision, &allowed)
            .await
            .unwrap()
            .is_none());
    }
}

#[test]
fn wiki_typed_dependency_futures_keep_record_checks_out_of_traversal_state() {
    fn bytes<A, F: std::future::Future>(_: impl FnOnce(A) -> F) -> usize {
        std::mem::size_of::<F>()
    }
    macro_rules! record_future_bytes {
        ($record:ty) => {
            bytes(
                |(reader, dependency, record, authorize): (
                    &'static ReadEligibility,
                    &'static elephant_types::records::KnowledgeDependency,
                    $record,
                    &'static ReadAuthorization<'static>,
                )| {
                    wiki_typed_dependency_is_current(reader, dependency, record, authorize)
                },
            )
        };
    }
    // Inspect the real typed children without constructing storage or polling.
    // Actual stack behavior still belongs to the unchanged Definition/erasure
    // scenarios; the async-trait box alone cannot bound its poll frame.
    for size in [
        record_future_bytes!(Entity),
        record_future_bytes!(Rel),
        record_future_bytes!(elephant_types::records::KnowledgeViewDefinition),
    ] {
        assert!(
            size <= 32 * 1024,
            "typed wiki dependency future is {size} bytes"
        );
    }
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
async fn wiki_typed_dependency_timestamp_refuses_before_domain_authorization() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    for embedded in [true, false] {
        let storage = client(embedded).await;
        let entity = crate::EntityRepository::new(storage.clone())
            .create(Entity::new(
                base(TableName::Entity),
                EntityType::new("person"),
                "Ada".into(),
            ))
            .await
            .unwrap();
        let reader = ReadEligibility::new(storage);
        let mut dependency = elephant_types::records::KnowledgeDependency {
            record_id: entity.base.id.to_string(),
            record_type: "entity".into(),
            role: "selected_member".into(),
            observed_updated_at: Some(entity.base.updated_at - chrono::Duration::seconds(1)),
        };
        let authorizations = AtomicUsize::new(0);
        let allow = |_: &BaseFields| {
            authorizations.fetch_add(1, Ordering::SeqCst);
            true
        };
        assert!(
            !wiki_typed_dependency_is_current(&reader, &dependency, entity.clone(), &allow,)
                .await
                .unwrap()
        );
        assert_eq!(authorizations.load(Ordering::SeqCst), 0);

        dependency.observed_updated_at = Some(entity.base.updated_at);
        assert!(
            wiki_typed_dependency_is_current(&reader, &dependency, entity.clone(), &allow,)
                .await
                .unwrap()
        );
        assert!(authorizations.load(Ordering::SeqCst) > 0);
        assert!(
            !wiki_typed_dependency_is_current(&reader, &dependency, entity.clone(), &|_| false,)
                .await
                .unwrap()
        );

        // A missing timestamp retains the original optional-observation rule;
        // it does not waive current domain authorization or deleted-row refusal.
        dependency.observed_updated_at = None;
        assert!(
            wiki_typed_dependency_is_current(&reader, &dependency, entity.clone(), &allow,)
                .await
                .unwrap()
        );
        let mut deleted = entity;
        deleted.base.deleted_at = Some(Utc::now());
        assert!(
            !wiki_typed_dependency_is_current(&reader, &dependency, deleted, &allow)
                .await
                .unwrap()
        );
    }
}
