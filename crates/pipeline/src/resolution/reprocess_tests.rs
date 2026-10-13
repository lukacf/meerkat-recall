//! Retained immutable-source and shared-support oracles; no provider calls.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use super::*;
use async_trait::async_trait;
use elephant_storage::{DocRepository, DocRevisionRepository, SourceRepository};
use elephant_types::records::assertion::{AssertionRetryState, EvidenceSpan};
use elephant_types::records::doc::Doc;
use elephant_types::records::doc_revision::{DocRevision, RendererMetadata, RevisionStatus};
use elephant_types::records::entity::EntityType;
use elephant_types::records::source::SourceType;
use std::sync::atomic::{AtomicUsize, Ordering};

const SPACE: &str = "reresolve-oracle";

fn base(table: TableName) -> BaseFields {
    let mut base = BaseFields::new(RecordId::new_v4(table), SPACE.into(), "oracle".into());
    base.security.policy_id = "classified".into();
    base.security.level = 2;
    base.security.labels = vec!["engineering".into()];
    base
}

fn options() -> ReprocessResolutionOptions {
    ReprocessResolutionOptions::new("oracle", RecordId::new_v4(TableName::WorkRun))
}

fn assertion(
    subject: RecordId,
    object: RecordId,
    revision: RecordId,
    source: RecordId,
) -> Assertion {
    let text = "Ada joined Research.";
    let mut base = base(TableName::Assertion);
    base.provenance.sources = vec![source];
    base.provenance.chain.push(ProvenanceStep {
        step: "grounded_promotion".into(),
        input: revision.to_string(),
        output: base.id.to_string(),
    });
    base.subjects = vec![Link::new(subject.clone())];
    Assertion {
        base,
        subject_text: "Ada".into(),
        predicate_raw: "joined".into(),
        predicate_id: Some("joined".into()),
        object_text: Some("Research".into()),
        object_literal: None,
        evidence: EvidenceSpan {
            revision: Link::new(revision),
            node: None,
            start: 0,
            end: u32::try_from(text.len()).unwrap(),
            quote_hash: crate::extraction::AssertionValidator::hash_quote(text),
        },
        source_event_time: None,
        observed_at: Utc::now(),
        asserted_at: Utc::now(),
        assertion_key: uuid::Uuid::new_v4().to_string(),
        resolution_status: ResolutionStatus::Resolved,
        resolution_method: Some("retained".into()),
        resolution_confidence: Some(0.9),
        retry_state: AssertionRetryState::default(),
        subject_entity: Some(Link::new(subject)),
        object_entity: Some(Link::new(object)),
        confidence: 0.9,
        subject_entity_type: Some("person".into()),
        object_entity_type: Some("org".into()),
        significance: Some("high".into()),
        lifecycle_tier: "active".into(),
    }
}

struct Fixture {
    storage: StorageClient,
    source: Source,
    revision: DocRevision,
    subject: Entity,
    assertion: Assertion,
    rel: Rel,
    event: Event,
}

async fn fixture() -> Fixture {
    fixture_on(StorageClient::new_embedded().await.unwrap()).await
}

async fn register_space(storage: &StorageClient) {
    let configuration = elephant_types::records::SpaceConfig {
        base: BaseFields::new(
            RecordId::new_v4(TableName::SpaceConfig),
            SPACE.into(),
            "oracle".into(),
        ),
        embedding_provider: "not-dispatched".into(),
        embedding_model: "fixture".into(),
        embedding_dims: 8,
        processing: None,
        purpose: None,
        skill: None,
    };
    elephant_storage::SpaceConfigRepository::new(storage.clone())
        .create(configuration)
        .await
        .unwrap();
}

async fn fixture_on(storage: StorageClient) -> Fixture {
    register_space(&storage).await;
    let text = "Ada joined Research.";
    let hash = crate::extraction::AssertionValidator::hash_quote(text);
    let source = SourceRepository::new(storage.clone())
        .create(Source::new(
            base(TableName::Source),
            SourceType::Manual,
            hash.clone(),
        ))
        .await
        .unwrap();
    let mut doc_base = base(TableName::Doc);
    doc_base.provenance.sources = vec![source.base.id.clone()];
    let doc = DocRepository::new(storage.clone())
        .create(Doc {
            base: doc_base,
            title: "Retained immutable evidence".into(),
            text: text.into(),
            sidecar_text: None,
            source: Some(Link::new(source.base.id.clone())),
            media_attachments: vec![],
            rendition_key: uuid::Uuid::new_v4().to_string(),
            metadata: serde_json::json!({}),
        })
        .await
        .unwrap();
    let mut revision_base = base(TableName::DocRevision);
    revision_base.provenance.sources = vec![source.base.id.clone()];
    let revision = DocRevisionRepository::new(storage.clone())
        .create(DocRevision {
            base: revision_base,
            doc: Link::new(doc.base.id),
            text: text.into(),
            text_hash: hash,
            renderer: RendererMetadata {
                name: "retained".into(),
                version: "1".into(),
                config_hash: None,
            },
            manifest: None,
            supersedes: None,
            status: RevisionStatus::Active,
            rendered_at: Utc::now(),
        })
        .await
        .unwrap();
    let entity_repo = EntityRepository::new(storage.clone());
    let mut entities = Vec::new();
    for (name, kind) in [("Ada", "person"), ("Research", "org")] {
        let mut entity_base = base(TableName::Entity);
        entity_base.provenance.sources = vec![source.base.id.clone()];
        entity_base.provenance.chain.push(ProvenanceStep {
            step: "grounded_promotion".into(),
            input: revision.base.id.to_string(),
            output: entity_base.id.to_string(),
        });
        let mut entity = Entity::new(entity_base, EntityType::new(kind), name.into());
        entity.identity_keys = vec![name.to_lowercase()];
        entities.push(entity_repo.create(entity).await.unwrap());
    }
    let subject = entities[0].clone();
    let assertion = AssertionRepository::new(storage.clone())
        .create(assertion(
            subject.base.id.clone(),
            entities[1].base.id.clone(),
            revision.base.id.clone(),
            source.base.id.clone(),
        ))
        .await
        .unwrap();
    let mut rel_base = base(TableName::Rel);
    inherit(&mut rel_base, &assertion.base);
    add_entity(&mut rel_base, &Link::new(entities[1].base.id.clone()));
    let rel = RelRepository::new(storage.clone())
        .create(Rel {
            base: rel_base,
            subject: Link::new(subject.base.id.clone()),
            object: Link::new(entities[1].base.id.clone()),
            rel_type: "joined".into(),
            status: ClaimStatus::Approved,
            confidence: 0.9,
            validity_window: None,
            evidence: vec![evidence_ref(&assertion)],
            temporal_audit: None,
        })
        .await
        .unwrap();
    let mut event_base = base(TableName::Event);
    inherit(&mut event_base, &rel.base);
    let mut event = Event::new(
        event_base,
        text,
        vec![Link::new(assertion.base.id.clone())],
        vec![Link::new(source.base.id.clone())],
    )
    .unwrap();
    event.status = EventStatus::Approved;
    let event = EventRepository::new(storage.clone())
        .create(event)
        .await
        .unwrap();
    for (target, kind) in [(&rel.base, "rel"), (&event.base, "event")] {
        let freshness = prepare_claim_freshness(
            None,
            &target.id,
            kind,
            target,
            ClaimFreshnessObservation {
                evidence_times: &[assertion.observed_at],
                evidence_strength: 0.9,
                conflict_severity: 0.0,
                corroboration_count: 1,
            },
        );
        FreshnessRepository::new(storage.clone())
            .create(freshness)
            .await
            .unwrap();
    }
    Fixture {
        storage,
        source,
        revision,
        subject,
        assertion,
        rel,
        event,
    }
}

#[derive(Debug)]
struct Choose {
    id: RecordId,
    calls: AtomicUsize,
}

#[async_trait]
impl PromotionDisambiguator for Choose {
    async fn disambiguate(
        &self,
        input: &DisambiguationRequest,
    ) -> PipelineResult<Option<PromotionDisambiguationDecision>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        assert_eq!(input.context_snippet, "Ada joined Research.");
        Ok(Some(PromotionDisambiguationDecision::Reuse {
            entity_id: self.id.to_string(),
        }))
    }
}

async fn competitor(f: &Fixture) -> Entity {
    let mut entity = f.subject.clone();
    entity.base.id = RecordId::new_v4(TableName::Entity);
    entity.base.provenance.chain[0].output = entity.base.id.to_string();
    EntityRepository::new(f.storage.clone())
        .create(entity)
        .await
        .unwrap()
}

#[tokio::test]
async fn already_resolved_rerun_preserves_approved_graph_and_unrelated_corpus() {
    let f = fixture().await;
    let other = competitor(&f).await;
    let mut unrelated = other.clone();
    unrelated.base.provenance.sources = vec![RecordId::new_v4(TableName::Source)];
    unrelated.base.security.policy_id = "another-policy".into();
    EntityRepository::new(f.storage.clone())
        .update(SPACE, &other.base.id, unrelated)
        .await
        .unwrap();
    let model = Arc::new(Choose {
        id: other.base.id,
        calls: AtomicUsize::new(0),
    });
    let mut options = options();
    options.disambiguator = Some(model.clone());
    let planning = PlanningTruthMaintenanceStorage::new(f.storage.clone(), SPACE, None)
        .await
        .unwrap();
    let result = prepare_reprocess(&planning, &f.storage, SPACE, &[f.source], &options)
        .await
        .unwrap();
    assert_eq!(result.assertions_reprocessed, 1);
    assert_eq!(model.calls.load(Ordering::SeqCst), 0);
    assert!(planning.prepared_plan().unwrap().effects.is_empty());
    assert_eq!(
        RelRepository::new(f.storage.clone())
            .get(SPACE, &f.rel.base.id)
            .await
            .unwrap(),
        Some(f.rel)
    );
    assert_eq!(
        EventRepository::new(f.storage)
            .get(SPACE, &f.event.base.id)
            .await
            .unwrap(),
        Some(f.event)
    );
}

#[tokio::test]
async fn changed_resolution_prepares_complete_effects_without_writes_or_new_assertions() {
    let f = fixture().await;
    let selected = competitor(&f).await;
    let mut options = options();
    options.disambiguator = Some(Arc::new(Choose {
        id: selected.base.id.clone(),
        calls: AtomicUsize::new(0),
    }));
    let planning = PlanningTruthMaintenanceStorage::new(f.storage.clone(), SPACE, None)
        .await
        .unwrap();
    let result = prepare_reprocess(&planning, &f.storage, SPACE, &[f.source], &options)
        .await
        .unwrap();
    assert_eq!((result.rels_superseded, result.events_superseded), (1, 1));
    let plan = planning.prepared_plan().unwrap();
    let changed = plan
        .effects
        .iter()
        .filter_map(|effect| effect.after.as_ref())
        .collect::<Vec<_>>();
    let assertion_effects = changed
        .iter()
        .filter_map(|row| match row {
            elephant_storage::canonical_commit::CanonicalRecord::Assertion(a) => Some(a),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(assertion_effects.len(), 1);
    assert_eq!(assertion_effects[0].base.id, f.assertion.base.id);
    assert_eq!(assertion_effects[0].evidence, f.assertion.evidence);
    assert_eq!(
        assertion_effects[0].subject_entity.as_ref().unwrap().id(),
        &selected.base.id
    );
    assert!(plan
        .effects
        .iter()
        .filter(|effect| effect
            .after
            .as_ref()
            .is_some_and(|row| row.base().id.table() == TableName::Assertion))
        .all(|effect| effect.before.is_some()));
    assert_eq!(
        changed
            .iter()
            .filter(|row| row.base().id.table() == TableName::Freshness)
            .count(),
        4
    );
    assert!(changed
        .iter()
        .all(|row| row.base().security == f.assertion.base.security));
    assert_eq!(
        AssertionRepository::new(f.storage.clone())
            .get(SPACE, &f.assertion.base.id)
            .await
            .unwrap(),
        Some(f.assertion)
    );
    assert_eq!(
        RelRepository::new(f.storage)
            .get(SPACE, &f.rel.base.id)
            .await
            .unwrap(),
        Some(f.rel)
    );
}

#[tokio::test]
async fn changed_resolution_commits_original_assertion_and_complete_graph_together() {
    for embedded in [true, false] {
        let storage = if embedded {
            StorageClient::new_embedded().await.unwrap()
        } else {
            let storage = StorageClient::new_memory().await.unwrap();
            elephant_migrations::apply_schema(&storage).await.unwrap();
            storage
        };
        let f = fixture_on(storage).await;
        let selected = competitor(&f).await;
        let mut options = options();
        options.disambiguator = Some(Arc::new(Choose {
            id: selected.base.id.clone(),
            calls: AtomicUsize::new(0),
        }));
        let planning = PlanningTruthMaintenanceStorage::new(f.storage.clone(), SPACE, None)
            .await
            .unwrap();
        let result = prepare_reprocess(&planning, &f.storage, SPACE, &[f.source], &options)
            .await
            .unwrap();
        planning
            .commit(
                "re_resolve",
                "complete-effects-oracle",
                &serde_json::json!({"work_run":options.work_run_id}),
                &result,
            )
            .await
            .unwrap();
        let assertion_repo = AssertionRepository::new(f.storage.clone());
        let current = assertion_repo
            .get(SPACE, &f.assertion.base.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            current.subject_entity.as_ref().unwrap().id(),
            &selected.base.id
        );
        assert_eq!(current.evidence, f.assertion.evidence);
        assert_eq!(assertion_repo.list(SPACE, 10, None).await.unwrap().len(), 1);
        let rels = RelRepository::new(f.storage.clone())
            .list(SPACE, 10, None)
            .await
            .unwrap();
        let current_rels = rels
            .iter()
            .filter(|row| row.base.deleted_at.is_none() && row.status != ClaimStatus::Superseded)
            .collect::<Vec<_>>();
        assert_eq!(current_rels.len(), 1);
        assert_ne!(current_rels[0].base.id, f.rel.base.id);
        assert_eq!(current_rels[0].subject.id(), &selected.base.id);
        assert_eq!(current_rels[0].evidence, f.rel.evidence);
        let events = EventRepository::new(f.storage.clone())
            .list(SPACE, 10, None)
            .await
            .unwrap();
        let current_events = events
            .iter()
            .filter(|row| row.base.deleted_at.is_none() && row.status != EventStatus::Superseded)
            .collect::<Vec<_>>();
        assert_eq!(current_events.len(), 1);
        assert_eq!(current_events[0].assertion_refs, f.event.assertion_refs);
        assert_eq!(current_events[0].title, f.event.title);
        let freshness = FreshnessRepository::new(f.storage.clone())
            .list(SPACE, 10, None)
            .await
            .unwrap();
        let live_targets = freshness
            .iter()
            .filter(|row| row.base.deleted_at.is_none())
            .map(|row| row.target.clone())
            .collect::<BTreeSet<_>>();
        assert_eq!(
            live_targets,
            BTreeSet::from([
                current_rels[0].base.id.to_string(),
                current_events[0].base.id.to_string()
            ])
        );
        let reader = ReadEligibility::new(f.storage);
        assert!(reader
            .project((*current_rels[0]).clone(), &|_| true)
            .await
            .unwrap()
            .is_some());
        assert!(reader
            .project((*current_events[0]).clone(), &|_| true)
            .await
            .unwrap()
            .is_some());
    }
}

#[tokio::test]
#[expect(
    clippy::cloned_ref_to_slice_refs,
    reason = "Retain the original owned source request and later live-state checks in this stale-revision regression."
)]
async fn stale_revision_and_foreign_model_choice_fail_without_live_changes() {
    let f = fixture().await;
    competitor(&f).await;
    let mut options = options();
    options.disambiguator = Some(Arc::new(Choose {
        id: RecordId::new_v4(TableName::Entity),
        calls: AtomicUsize::new(0),
    }));
    let planning = PlanningTruthMaintenanceStorage::new(f.storage.clone(), SPACE, None)
        .await
        .unwrap();
    assert!(
        prepare_reprocess(&planning, &f.storage, SPACE, &[f.source.clone()], &options)
            .await
            .is_err()
    );
    let mut changed = f.revision.clone();
    changed.text = "Ada never joined Research.".into();
    DocRevisionRepository::new(f.storage.clone())
        .update(SPACE, &changed.base.id, changed.clone())
        .await
        .unwrap();
    let planning = PlanningTruthMaintenanceStorage::new(f.storage.clone(), SPACE, None)
        .await
        .unwrap();
    assert!(
        prepare_reprocess(&planning, &f.storage, SPACE, &[f.source], &options)
            .await
            .is_err()
    );
    assert_eq!(
        RelRepository::new(f.storage)
            .get(SPACE, &f.rel.base.id)
            .await
            .unwrap(),
        Some(f.rel)
    );
}

#[tokio::test]
async fn original_epoch_rejects_race_after_preparation() {
    let f = fixture().await;
    let selected = competitor(&f).await;
    let mut options = options();
    options.disambiguator = Some(Arc::new(Choose {
        id: selected.base.id.clone(),
        calls: AtomicUsize::new(0),
    }));
    let planning = PlanningTruthMaintenanceStorage::new(f.storage.clone(), SPACE, None)
        .await
        .unwrap();
    let result = prepare_reprocess(&planning, &f.storage, SPACE, &[f.source], &options)
        .await
        .unwrap();
    let mut changed = selected;
    changed.summary = Some("concurrently edited".into());
    EntityRepository::new(f.storage.clone())
        .update(SPACE, &changed.base.id, changed.clone())
        .await
        .unwrap();
    assert!(planning
        .commit(
            "re_resolve",
            "oracle",
            &serde_json::json!({"run":options.work_run_id}),
            &result
        )
        .await
        .is_err());
    assert_eq!(
        AssertionRepository::new(f.storage.clone())
            .get(SPACE, &f.assertion.base.id)
            .await
            .unwrap(),
        Some(f.assertion)
    );
    assert_eq!(
        RelRepository::new(f.storage)
            .get(SPACE, &f.rel.base.id)
            .await
            .unwrap(),
        Some(f.rel)
    );
}

fn support_pair() -> (Assertions, Rel, RecordId) {
    let subject = RecordId::new_v4(TableName::Entity);
    let object = RecordId::new_v4(TableName::Entity);
    let a = assertion(
        subject.clone(),
        object.clone(),
        RecordId::new_v4(TableName::DocRevision),
        RecordId::new_v4(TableName::Source),
    );
    let b = assertion(
        subject.clone(),
        object.clone(),
        RecordId::new_v4(TableName::DocRevision),
        RecordId::new_v4(TableName::Source),
    );
    let mut after_a = a.clone();
    let replacement = RecordId::new_v4(TableName::Entity);
    after_a.subject_entity = Some(Link::new(replacement.clone()));
    add_entity(&mut after_a.base, &Link::new(replacement.clone()));
    let mut row_base = base(TableName::Rel);
    inherit(&mut row_base, &a.base);
    row_base
        .provenance
        .sources
        .extend(b.base.provenance.sources.iter().cloned());
    add_entity(&mut row_base, &Link::new(object.clone()));
    let row = Rel {
        base: row_base,
        subject: Link::new(subject),
        object: Link::new(object),
        rel_type: "joined".into(),
        status: ClaimStatus::Approved,
        confidence: 0.9,
        validity_window: None,
        evidence: vec![evidence_ref(&a), evidence_ref(&b)],
        temporal_audit: None,
    };
    let snapshots = Assertions {
        before: BTreeMap::from([
            (a.base.id.clone(), a.clone()),
            (b.base.id.clone(), b.clone()),
        ]),
        after: BTreeMap::from([(a.base.id.clone(), after_a), (b.base.id.clone(), b)]),
        changed: BTreeSet::from([a.base.id]),
    };
    (snapshots, row, replacement)
}

#[tokio::test]
async fn shared_support_is_partitioned_without_losing_unselected_source_or_order_dependence() {
    let storage = StorageClient::new_embedded().await.unwrap();
    register_space(&storage).await;
    let planning = PlanningTruthMaintenanceStorage::new(storage, SPACE, None)
        .await
        .unwrap();
    let (mut assertions, row, replacement) = support_pair();
    let a = rewrite_rel(&planning, &row, &mut assertions, &options())
        .await
        .unwrap();
    let mut reverse = row.clone();
    reverse.evidence.reverse();
    let b = rewrite_rel(&planning, &reverse, &mut assertions, &options())
        .await
        .unwrap();
    assert_eq!(a.len(), 2);
    assert_eq!(
        a.iter().map(|row| row.subject.id()).collect::<Vec<_>>(),
        b.iter().map(|row| row.subject.id()).collect::<Vec<_>>()
    );
    assert!(a.iter().any(|part| part.subject == row.subject));
    assert!(a.iter().any(|part| part.subject.id() == &replacement));
    let refs = a
        .iter()
        .flat_map(|part| part.evidence.iter().map(|e| e.assertion.id().clone()))
        .collect::<BTreeSet<_>>();
    assert_eq!(
        refs,
        row.evidence
            .iter()
            .map(|e| e.assertion.id().clone())
            .collect()
    );
    assert!(a
        .iter()
        .all(|part| part.base.provenance.sources == row.base.provenance.sources));
    assert!(a
        .iter()
        .all(|part| contains_authorization(&part.base, &row.base).unwrap()));
    assert!(planning.prepared_plan().unwrap().effects.is_empty());
}

#[tokio::test]
async fn attribute_reresolution_preserves_literal_type_and_rejects_incoherent_proof() {
    let storage = StorageClient::new_embedded().await.unwrap();
    register_space(&storage).await;
    let planning = PlanningTruthMaintenanceStorage::new(storage, SPACE, None)
        .await
        .unwrap();
    let (mut assertions, rel, _) = support_pair();
    for assertion in assertions
        .before
        .values_mut()
        .chain(assertions.after.values_mut())
    {
        assertion.object_text = None;
        assertion.object_entity = None;
        assertion.object_literal = Some(serde_json::json!("2025-03-01"));
        assertion.predicate_raw = "started_on".into();
        assertion.predicate_id = Some("started_on".into());
    }
    let mut row = Attribute {
        base: rel.base,
        subject: rel.subject,
        attr_type: "started_on".into(),
        value: Literal::Date("2025-03-01".into()),
        status: ClaimStatus::Approved,
        confidence: 0.9,
        validity_window: None,
        evidence: rel.evidence,
        temporal_audit: None,
    };
    let parts = rewrite_attribute(&planning, &row, &mut assertions, &options())
        .await
        .unwrap();
    assert_eq!(parts.len(), 2);
    assert!(parts
        .iter()
        .all(|part| part.value == Literal::Date("2025-03-01".into())));
    row.value = Literal::Date("2025-04-01".into());
    assert!(
        rewrite_attribute(&planning, &row, &mut assertions, &options())
            .await
            .is_err()
    );
}
