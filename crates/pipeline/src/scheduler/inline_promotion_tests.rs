//! Real immutable-lineage fixtures for promotion planning. Atomic publication,
//! attempt races and complete-source receipts are tested by canonical/finalizer
//! suites; these tests inspect uncommitted plans and require zero live effects.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
use super::*;
use crate::extraction::{
    verify_assertion_support, AssertionSupportVerifier, AssertionValidator, SupportCandidate,
    SupportVerdict,
};
use crate::renderer::{default::PassthroughRenderer, RendererRegistry};
use elephant_storage::{
    DocNodeRepository, DocRepository, DocRevisionRepository, Repository, SourceRepository,
};
use elephant_types::records::doc::Doc;
use elephant_types::records::doc_node::{DocNode, NodeRole, TextSpan};
use elephant_types::records::source::{Source, SourceType};

const SPACE: &str = "lab_notes";
const TEXT: &str = "Åsa works at Northstar. Åsa has role engineer. The sensor records 17 degrees.";
const QUOTE: &str = "Åsa works at Northstar.";

/// Fixed oracle for this fixture; does not accept arbitrary extractor output.
struct FixtureVerifier;
#[async_trait::async_trait]
impl AssertionSupportVerifier for FixtureVerifier {
    fn identity(&self) -> &str {
        "fixture-northstar-v1"
    }
    async fn verify(
        &self,
        candidates: &[SupportCandidate],
    ) -> Result<Vec<SupportVerdict>, PipelineError> {
        Ok(candidates
            .iter()
            .map(|candidate| SupportVerdict {
                index: candidate.index,
                supported: candidate.quote == QUOTE
                    && candidate.claim["subject"] == "Åsa"
                    && candidate.claim["predicate"] == "works_at"
                    && candidate.claim["object"] == "Northstar"
                    && candidate.claim["event_time"].is_null()
                    && candidate.claim["event_title"].is_null(),
            })
            .collect())
    }
}

struct Fixture {
    storage: StorageClient,
    lineage: GroundingLineage,
    raw: RawAssertion,
    run: RecordId,
}
impl Fixture {
    async fn new() -> Self {
        Self::with_doc_metadata(serde_json::json!({})).await
    }
    async fn with_doc_metadata(doc_metadata: serde_json::Value) -> Self {
        let storage = StorageClient::new_embedded()
            .await
            .expect("embedded fixture");
        let mut value = serde_json::to_value(BaseFields::new(
            RecordId::new_v4(TableName::SpaceConfig),
            SPACE.into(),
            "fixture".into(),
        ))
        .expect("space fields");
        value["embedding_provider"] = serde_json::json!("not-dispatched");
        value["embedding_model"] = serde_json::json!("fixture");
        value["embedding_dims"] = serde_json::json!(8);
        value["processing"] = serde_json::Value::Null;
        elephant_storage::SpaceConfigRepository::new(storage.clone())
            .create(
                serde_json::from_value::<elephant_types::records::SpaceConfig>(value)
                    .expect("space config"),
            )
            .await
            .expect("register fixture space");

        let mut source_base = BaseFields::new(
            RecordId::new_v4(TableName::Source),
            SPACE.into(),
            "fixture".into(),
        );
        source_base.security = Security {
            policy_id: "lab-policy-v1".into(),
            level: 2,
            labels: vec!["research".into()],
            handling: vec!["no_export".into()],
        };
        source_base.subjects = vec![Link::new(RecordId::new_v4(TableName::Entity))];
        source_base.subjects_state = SubjectsState::Partial;
        let source = Source::new(
            source_base,
            SourceType::Manual,
            AssertionValidator::hash_quote(TEXT),
        );
        SourceRepository::new(storage.clone())
            .create(source.clone())
            .await
            .expect("source");
        let mut doc_base = source.base.clone();
        doc_base.id = RecordId::new_v4(TableName::Doc);
        let doc = Doc {
            base: doc_base,
            title: "Research observations".into(),
            text: TEXT.into(),
            sidecar_text: None,
            source: Some(Link::new(source.base.id.clone())),
            media_attachments: vec![],
            rendition_key: "lab-rendition".into(),
            metadata: doc_metadata,
        };
        DocRepository::new(storage.clone())
            .create(doc.clone())
            .await
            .expect("doc");
        let mut renderers = RendererRegistry::new();
        renderers.register(Box::new(PassthroughRenderer::new()));
        let rendered = super::super::evidence_work_kinds::execute_render_doc(
            &storage,
            &renderers,
            &doc,
            Some("text/plain"),
        )
        .await
        .expect("render real revision");
        let revision = DocRevisionRepository::new(storage.clone())
            .get_by_id(&rendered.revision_id)
            .await
            .unwrap()
            .unwrap();
        let mut node_base = revision.base.clone();
        node_base.id = RecordId::new_v4(TableName::DocNode);
        let node = DocNode {
            base: node_base,
            revision: Link::new(revision.base.id.clone()),
            role: NodeRole::Paragraph,
            span: TextSpan::new(0, TEXT.len() as u32),
            node_key: "lab-paragraph".into(),
            text: None,
            parent: None,
            position: 0,
            depth: 0,
            embedding_model: None,
            collection: None,
        };
        DocNodeRepository::new(storage.clone())
            .create(node.clone())
            .await
            .expect("node");
        let mut raw = RawAssertion {
            support_verification: None,
            subject_text: "Åsa".into(),
            predicate_text: "works_at".into(),
            object_text: Some("Northstar".into()),
            object_literal: None,
            evidence_span: EvidenceSpan {
                revision: Link::new(revision.base.id.clone()),
                node: Some(Link::new(node.base.id.clone())),
                start: 0,
                end: QUOTE.len() as u32,
                quote_hash: AssertionValidator::hash_quote(QUOTE),
            },
            confidence: 0.97,
            quote: Some(QUOTE.into()),
            source_event_time: None,
            source_event_time_precision: None,
            event_title: None,
            event_lineage_key: None,
            subject_entity_id: None,
            subject_entity_type: Some("person".into()),
            object_entity_id: None,
            object_entity_type: Some("org".into()),
            significance: Some("high".into()),
        };
        verify_assertion_support(std::slice::from_mut(&mut raw), TEXT, &FixtureVerifier)
            .await
            .expect("independent fixture verdict");
        Self {
            storage,
            lineage: GroundingLineage {
                source,
                doc,
                revision,
                node,
            },
            raw,
            run: RecordId::new_v4(TableName::WorkRun),
        }
    }
    async fn prepare(&self, raw: &[RawAssertion]) -> Result<PreparedPromotion, PipelineError> {
        prepare_assertions_inline_with_options(
            &self.storage,
            SPACE,
            raw,
            &self.run,
            &self.lineage.node.base.id,
            &self.lineage.revision.base.id,
            "fixture",
            &PromotionOptions::default(),
        )
        .await
    }
    async fn no_live_effects(&self) {
        assert!(EntityRepository::new(self.storage.clone())
            .list(SPACE, 100, None)
            .await
            .unwrap()
            .is_empty());
        assert!(AssertionRepository::new(self.storage.clone())
            .list(SPACE, 100, None)
            .await
            .unwrap()
            .is_empty());
        assert!(RelRepository::new(self.storage.clone())
            .list(SPACE, 100, None)
            .await
            .unwrap()
            .is_empty());
        assert!(AttributeRepository::new(self.storage.clone())
            .list(SPACE, 100, None)
            .await
            .unwrap()
            .is_empty());
        assert!(EventRepository::new(self.storage.clone())
            .list(SPACE, 100, None)
            .await
            .unwrap()
            .is_empty());
    }
}

#[tokio::test]
#[expect(
    clippy::cloned_ref_to_slice_refs,
    reason = "Retain the owned evidence fixture in the original source-restriction and no-live-write oracle."
)]
async fn grounded_plan_inherits_all_source_restrictions_without_live_writes() {
    let fixture = Fixture::new().await;
    let prepared = fixture
        .prepare(&[fixture.raw.clone()])
        .await
        .expect("prepare");
    assert_eq!(prepared.result.entities_created, 2);
    assert_eq!(prepared.result.assertions_recorded, 1);
    assert_eq!(prepared.result.rels_created, 1);
    assert_eq!(prepared.result.events_created, 1);
    assert_eq!(prepared.plan.outbox.len(), 1);
    assert_eq!(
        prepared
            .plan
            .dependencies
            .iter()
            .filter(|record| matches!(record, CanonicalRecord::Source(_)))
            .count(),
        1
    );
    for effect in &prepared.plan.effects {
        let base = effect.after.as_ref().expect("creation").base();
        assert_eq!(base.security, fixture.lineage.source.base.security);
        assert_eq!(base.subjects_state, SubjectsState::Partial);
        assert!(base
            .subjects
            .iter()
            .any(|subject| subject == &fixture.lineage.source.base.subjects[0]));
        assert_eq!(
            base.provenance.sources,
            vec![fixture.lineage.source.base.id.clone()]
        );
    }
    fixture.no_live_effects().await;
}

#[tokio::test]
#[expect(
    clippy::cloned_ref_to_slice_refs,
    reason = "Retain the owned baseline evidence and original accepted-prefix composition oracle."
)]
async fn accepted_prefix_is_read_before_promotion_and_composes_without_live_writes() {
    let fixture = Fixture::new().await;
    // Obtain a grounded entity through the actual planner. This is an overlay
    // composition oracle, not a substitute for the owner's contact-admission
    // or atomic publication tests.
    let baseline = fixture.prepare(&[fixture.raw.clone()]).await.unwrap();
    let mut accepted = baseline
        .plan
        .effects
        .iter()
        .find_map(|effect| match effect.after.as_ref() {
            Some(CanonicalRecord::Entity(entity)) if entity.display_name == "Åsa" => {
                Some((**entity).clone())
            }
            _ => None,
        })
        .unwrap();
    accepted.metadata["accepted_contact_field"] = serde_json::json!("captured original value");
    accepted.last_seen = None;
    let entity_id = accepted.base.id.clone();
    let mut accepted_event = baseline.plan.outbox[0].clone();
    accepted_event.base.id = RecordId::new_v4(TableName::SystemEvent);
    accepted_event.event_type = "contact.ingested".into();
    accepted_event.payload = serde_json::json!({
        "space_id": SPACE,
        "event_type": "contact.ingested",
        "actor": "fixture",
        "records": [{"id": entity_id, "table": "entity", "op": "create"}],
    });
    let mut prefix = CanonicalPlan {
        observed_epoch: baseline.plan.observed_epoch.clone(),
        dependencies: baseline.plan.dependencies.clone(),
        outbox: vec![accepted_event],
        ..CanonicalPlan::default()
    };
    prefix
        .push(None, Some(CanonicalRecord::Entity(Box::new(accepted))))
        .unwrap();
    let initial_outbox = prefix.outbox.clone();
    let original_epoch = prefix.observed_epoch.clone();
    let mut raw = fixture.raw.clone();
    raw.subject_entity_id = Some(entity_id.to_string());
    verify_assertion_support(std::slice::from_mut(&mut raw), TEXT, &FixtureVerifier)
        .await
        .unwrap();
    let prepared = prepare_assertion_groups_with_plan(
        &fixture.storage,
        SPACE,
        &[PromotionGroup {
            assertions: vec![raw],
            work_run_id: fixture.run.clone(),
            doc_node_id: fixture.lineage.node.base.id.clone(),
            revision_id: fixture.lineage.revision.base.id.clone(),
        }],
        "fixture",
        &PromotionOptions::default(),
        prefix,
    )
    .await
    .unwrap();
    assert_eq!(prepared.plan.observed_epoch, original_epoch);
    assert_eq!(
        prepared.result.entities_created, 1,
        "reuse the seeded subject"
    );
    assert!(prepared.plan.outbox.starts_with(&initial_outbox));
    let effects: Vec<_> = prepared
        .plan
        .effects
        .iter()
        .filter(|effect| {
            effect
                .after
                .as_ref()
                .is_some_and(|record| record.base().id == entity_id)
        })
        .collect();
    assert_eq!(
        effects.len(),
        1,
        "coalesce, do not append conflicting writes"
    );
    assert!(
        effects[0].before.is_none(),
        "keep the original absent before-image"
    );
    let Some(CanonicalRecord::Entity(after)) = effects[0].after.as_ref() else {
        panic!("expected seeded entity");
    };
    assert_eq!(
        after.metadata["accepted_contact_field"],
        "captured original value"
    );
    assert!(
        after.last_seen.is_some(),
        "promotion enrichment also survives"
    );
    assert!(same_support_partition(
        &after.base,
        &lineage_entity_base(&fixture.lineage)
    ));
    fixture.no_live_effects().await;
}

#[tokio::test]
#[expect(
    clippy::cloned_ref_to_slice_refs,
    reason = "Retain the owned baseline evidence in the original cross-space prefix rejection oracle."
)]
async fn promotion_rejects_cross_space_prefix_without_replacing_its_authority() {
    let fixture = Fixture::new().await;
    let baseline = fixture.prepare(&[fixture.raw.clone()]).await.unwrap();
    let mut prefix = CanonicalPlan {
        observed_epoch: baseline.plan.observed_epoch,
        ..CanonicalPlan::default()
    };
    let mut foreign = fixture.lineage.source.clone();
    foreign.base.space_id = "another-space".into();
    prefix
        .dependencies
        .push(CanonicalRecord::Source(Box::new(foreign)));
    let result = prepare_assertion_groups_with_plan(
        &fixture.storage,
        SPACE,
        &[PromotionGroup {
            assertions: vec![fixture.raw.clone()],
            work_run_id: fixture.run.clone(),
            doc_node_id: fixture.lineage.node.base.id.clone(),
            revision_id: fixture.lineage.revision.base.id.clone(),
        }],
        "fixture",
        &PromotionOptions::default(),
        prefix,
    )
    .await;
    assert!(matches!(result, Err(PipelineError::ValidationError(_))));
    fixture.no_live_effects().await;
}

#[tokio::test]
#[expect(
    clippy::cloned_ref_to_slice_refs,
    reason = "Retain the owned baseline evidence in the original epoch and attempt-fence rejection oracle."
)]
async fn seeded_promotion_rejects_missing_stale_epoch_and_foreign_attempt() {
    let fixture = Fixture::new().await;
    let baseline = fixture.prepare(&[fixture.raw.clone()]).await.unwrap();
    let original = CanonicalPlan {
        observed_epoch: baseline.plan.observed_epoch,
        ..CanonicalPlan::default()
    };
    let mut stale = original.clone();
    stale.observed_epoch = "obsolete-original-epoch".into();
    let foreign = PromotionOptions {
        publication: Some(
            elephant_storage::canonical_commit::CanonicalPublicationContext {
                source: elephant_types::publication::PublicationFence {
                    source_id: fixture.lineage.source.base.id.clone(),
                    generation: "captured-generation".into(),
                    revision_id: "captured-revision".into(),
                    operation_id: "captured-operation".into(),
                },
                attempt: elephant_types::publication::AttemptFence {
                    space_id: "another-space".into(),
                    work_item_id: RecordId::new_v4(TableName::WorkItem),
                    generation: 1,
                    lease_token: "captured-token".into(),
                },
            },
        ),
        ..PromotionOptions::default()
    };
    for (prefix, options, concurrent) in [
        (CanonicalPlan::default(), PromotionOptions::default(), false),
        (stale, PromotionOptions::default(), true),
        (original, foreign, false),
    ] {
        let result = prepare_assertion_groups_with_plan(
            &fixture.storage,
            SPACE,
            &[PromotionGroup {
                assertions: vec![fixture.raw.clone()],
                work_run_id: fixture.run.clone(),
                doc_node_id: fixture.lineage.node.base.id.clone(),
                revision_id: fixture.lineage.revision.base.id.clone(),
            }],
            "fixture",
            &options,
            prefix,
        )
        .await;
        if concurrent {
            assert!(matches!(result, Err(PipelineError::StorageError(
                elephant_storage::StorageError::Conflict(code))) if code == "canonical_epoch_changed"));
        } else {
            assert!(matches!(result, Err(PipelineError::ValidationError(_))));
        }
    }
    fixture.no_live_effects().await;
}

#[tokio::test]
async fn promotion_prefix_refuses_real_competing_write_without_refreshing_epoch() {
    let fixture = Fixture::new().await;
    let canonical =
        elephant_storage::canonical_commit::CanonicalRepository::new(fixture.storage.clone());
    let observed = canonical.epoch(SPACE).await.unwrap();
    let mut unrelated = fixture.lineage.source.clone();
    unrelated.base.id = RecordId::new_v4(TableName::Source);
    SourceRepository::new(fixture.storage.clone())
        .create(unrelated)
        .await
        .unwrap();
    let current = canonical.epoch(SPACE).await.unwrap();
    assert_ne!(observed, current);
    let group = PromotionGroup {
        assertions: vec![fixture.raw.clone()],
        work_run_id: fixture.run.clone(),
        doc_node_id: fixture.lineage.node.base.id.clone(),
        revision_id: fixture.lineage.revision.base.id.clone(),
    };
    let stale = Box::pin(prepare_assertion_groups_with_plan(
        &fixture.storage,
        SPACE,
        std::slice::from_ref(&group),
        "fixture",
        &PromotionOptions::default(),
        CanonicalPlan {
            observed_epoch: observed,
            ..CanonicalPlan::default()
        },
    ))
    .await;
    assert!(matches!(stale, Err(PipelineError::StorageError(
        elephant_storage::StorageError::Conflict(code))) if code == "canonical_epoch_changed"));
    assert_eq!(
        canonical.epoch(SPACE).await.unwrap(),
        current,
        "refusal cannot refresh or write the original plan"
    );
    fixture.no_live_effects().await;
    // A separate fresh preparation still validates exact immutable evidence;
    // the failed preparation itself never adopts the new observation.
    Box::pin(prepare_assertion_groups_with_plan(
        &fixture.storage,
        SPACE,
        &[group],
        "fixture",
        &PromotionOptions::default(),
        CanonicalPlan {
            observed_epoch: current,
            ..CanonicalPlan::default()
        },
    ))
    .await
    .unwrap();
    fixture.no_live_effects().await;
}

#[tokio::test]
async fn forged_or_unsupported_batch_never_prepares_any_live_effect() {
    let fixture = Fixture::new().await;
    let mut cases = Vec::new();
    let mut raw = fixture.raw.clone();
    raw.evidence_span.end = raw.evidence_span.start;
    cases.push(raw);
    let mut raw = fixture.raw.clone();
    raw.evidence_span.end = u32::MAX;
    cases.push(raw);
    let mut raw = fixture.raw.clone();
    raw.evidence_span.start = 1;
    cases.push(raw); // inside Å
    let mut raw = fixture.raw.clone();
    raw.evidence_span.quote_hash = "derived:invented".into();
    cases.push(raw);
    let mut raw = fixture.raw.clone();
    raw.evidence_span.revision = Link::new(RecordId::new_v4(TableName::DocRevision));
    cases.push(raw);
    let mut raw = fixture.raw.clone();
    raw.evidence_span.node = Some(Link::new(RecordId::new_v4(TableName::DocNode)));
    cases.push(raw);
    let mut raw = fixture.raw.clone();
    raw.support_verification = None;
    cases.push(raw);
    let mut raw = fixture.raw.clone();
    raw.predicate_text = "owns".into();
    cases.push(raw);
    let mut raw = fixture.raw.clone();
    raw.confidence = f64::NAN;
    cases.push(raw);
    let mut raw = fixture.raw.clone();
    raw.event_title = Some("Northstar acquired Åsa".into());
    cases.push(raw);
    for raw in cases {
        assert!(fixture.prepare(&[fixture.raw.clone(), raw]).await.is_err());
        fixture.no_live_effects().await;
    }
}

#[tokio::test]
async fn perfectly_valid_quote_with_unsupported_interpretation_is_not_approved() {
    let fixture = Fixture::new().await;
    let mut raw = fixture.raw.clone();
    raw.predicate_text = "owns".into();
    raw.confidence = 1.0;
    verify_assertion_support(std::slice::from_mut(&mut raw), TEXT, &FixtureVerifier)
        .await
        .unwrap();
    assert!(raw.support_verification.is_none());
    assert!(fixture.prepare(&[raw]).await.is_err());
    fixture.no_live_effects().await;
}

#[tokio::test]
#[expect(
    clippy::cloned_ref_to_slice_refs,
    reason = "Retain the owned evidence snapshots in both original source mutation rejection cases."
)]
async fn source_deletion_and_changed_source_content_fence_prepared_interpretation() {
    let fixture = Fixture::new().await;
    let repository = SourceRepository::new(fixture.storage.clone());
    let mut changed = fixture.lineage.source.clone();
    changed.hash = AssertionValidator::hash_quote("new revision");
    repository
        .update(SPACE, &changed.base.id, changed.clone())
        .await
        .unwrap();
    assert!(fixture.prepare(&[fixture.raw.clone()]).await.is_err());
    repository.delete(SPACE, &changed.base.id).await.unwrap();
    assert!(fixture.prepare(&[fixture.raw.clone()]).await.is_err());
    fixture.no_live_effects().await;
}

#[tokio::test]
#[expect(
    clippy::cloned_ref_to_slice_refs,
    reason = "Retain the owned evidence fixture in the original document and revision rejection oracle."
)]
async fn changed_or_deleted_doc_and_superseded_revision_fail_closed() {
    let fixture = Fixture::new().await;
    let mut changed = fixture.lineage.clone();
    changed.doc.base.security.level = 0;
    assert!(changed.validate(SPACE).is_err());
    let mut changed = fixture.lineage.clone();
    changed.revision.status = elephant_types::records::doc_revision::RevisionStatus::Superseded;
    assert!(changed.validate(SPACE).is_err());
    let mut changed = fixture.lineage.clone();
    changed.revision.base.space_id = "another_lab".into();
    assert!(changed.validate(SPACE).is_err());
    let mut changed = fixture.lineage.clone();
    changed.source.base.subjects.clear();
    assert!(changed.validate(SPACE).is_err());
    DocRepository::new(fixture.storage.clone())
        .delete(SPACE, &fixture.lineage.doc.base.id)
        .await
        .unwrap();
    assert!(fixture.prepare(&[fixture.raw.clone()]).await.is_err());
    fixture.no_live_effects().await;
}

#[tokio::test]
#[expect(
    clippy::cloned_ref_to_slice_refs,
    reason = "Retain the original public promoter call shape and owned fixture used to reject missing authority."
)]
async fn default_public_promoter_cannot_bypass_source_manifest_or_attempt_fence() {
    let fixture = Fixture::new().await;
    assert!(promote_assertions_inline(
        &fixture.storage,
        SPACE,
        &[fixture.raw.clone()],
        &fixture.run,
        &fixture.lineage.node.base.id,
        &fixture.lineage.revision.base.id,
        "fixture"
    )
    .await
    .is_err());
    fixture.no_live_effects().await;
}

#[tokio::test]
async fn staging_roundtrip_preserves_support_but_never_invents_it() {
    let fixture = Fixture::new().await;
    let mut serialized =
        super::super::extraction_work_kinds::SerializedRawAssertion::from_raw(&fixture.raw)
            .unwrap();
    let roundtrip = serialized
        .to_raw(
            &fixture.lineage.revision.base.id,
            &fixture.lineage.node.base.id,
        )
        .unwrap();
    roundtrip.validate_support().unwrap();
    serialized.quote_hash = None;
    assert!(serialized
        .to_raw(
            &fixture.lineage.revision.base.id,
            &fixture.lineage.node.base.id
        )
        .is_err());
}

#[tokio::test]
#[expect(
    clippy::cloned_ref_to_slice_refs,
    reason = "Retain the owned evidence fixture in the original unrelated-entity mutation oracle."
)]
async fn unrelated_entities_and_other_source_mentions_are_never_enriched() {
    let fixture = Fixture::new().await;
    let mut base = BaseFields::new(
        RecordId::new_v4(TableName::Entity),
        SPACE.into(),
        "fixture".into(),
    );
    fixture.lineage.inherit(&mut base);
    base.provenance.sources = vec![RecordId::new_v4(TableName::Source)];
    let mut unrelated = Entity::new(base, EntityType::new("person"), "Åsa".into());
    unrelated.identity_keys = vec!["åsa".into()];
    EntityRepository::new(fixture.storage.clone())
        .create(unrelated.clone())
        .await
        .unwrap();
    let prepared = fixture.prepare(&[fixture.raw.clone()]).await.unwrap();
    assert_eq!(prepared.result.entities_created, 2);
    assert!(prepared
        .plan
        .effects
        .iter()
        .all(|effect| effect.after.as_ref().unwrap().base().id != unrelated.base.id));
    assert_eq!(
        EntityRepository::new(fixture.storage.clone())
            .get(SPACE, &unrelated.base.id)
            .await
            .unwrap()
            .unwrap(),
        unrelated
    );
}

#[tokio::test]
#[expect(
    clippy::cloned_ref_to_slice_refs,
    reason = "Retain the owned evidence fixture in the original identity reuse and planned topic-upgrade oracle."
)]
async fn same_partition_identity_reuse_and_topic_upgrade_stay_planned() {
    let fixture = Fixture::new().await;
    let mut base = BaseFields::new(
        RecordId::new_v4(TableName::Entity),
        SPACE.into(),
        "fixture".into(),
    );
    fixture.lineage.inherit(&mut base);
    let mut existing = Entity::new(base, EntityType::new("topic"), "Åsa".into());
    existing.identity_keys = vec!["åsa".into()];
    EntityRepository::new(fixture.storage.clone())
        .create(existing.clone())
        .await
        .unwrap();
    let prepared = fixture.prepare(&[fixture.raw.clone()]).await.unwrap();
    assert_eq!(prepared.result.entities_created, 1);
    let updated = prepared
        .plan
        .effects
        .iter()
        .filter_map(|effect| effect.after.as_ref())
        .find_map(|record| {
            if let CanonicalRecord::Entity(entity) = record {
                (entity.base.id == existing.base.id).then_some(entity)
            } else {
                None
            }
        })
        .expect("planned entity upgrade");
    assert_eq!(updated.entity_type, EntityType::new("person"));
    assert_eq!(
        EntityRepository::new(fixture.storage.clone())
            .get(SPACE, &existing.base.id)
            .await
            .unwrap()
            .unwrap()
            .entity_type,
        EntityType::new("topic")
    );
}

#[tokio::test]
async fn full_envelope_partitions_are_order_independent() {
    let fixture = Fixture::new().await;
    let mut base = lineage_entity_base(&fixture.lineage);
    base.security.labels.push("second-label".into());
    base.subjects
        .push(Link::new(RecordId::new_v4(TableName::Entity)));
    base.provenance
        .sources
        .push(RecordId::new_v4(TableName::Source));
    let mut reordered = base.clone();
    reordered.security.labels.reverse();
    reordered.subjects.reverse();
    reordered.provenance.sources.reverse();
    assert!(same_support_partition(&base, &reordered));
    for field in [
        "policy",
        "level",
        "subjects",
        "completeness",
        "source",
        "revision",
    ] {
        let mut different = base.clone();
        match field {
            "policy" => different.security.policy_id = "another-policy".into(),
            "level" => different.security.level += 1,
            "subjects" => different.subjects.clear(),
            "completeness" => different.subjects_state = SubjectsState::Complete,
            "revision" => different
                .provenance
                .chain
                .iter_mut()
                .filter(|step| step.step == "grounded_promotion")
                .for_each(|step| step.input = RecordId::new_v4(TableName::DocRevision).to_string()),
            _ => different.provenance.sources.clear(),
        }
        assert!(!same_support_partition(&base, &different));
        assert!(!same_support_partition(&different, &base));
    }
}

#[test]
fn distinct_policy_ids_cannot_be_conjoined_in_either_order() {
    let left = Security {
        policy_id: "policy-a".into(),
        ..Security::default()
    };
    let right = Security {
        policy_id: "policy-b".into(),
        ..Security::default()
    };
    assert!(escalate_security(&mut left.clone(), Some(&right)).is_err());
    assert!(escalate_security(&mut right.clone(), Some(&left)).is_err());
}

#[test]
fn entity_types_compatible_matrix() {
    let person = EntityType::new("person");
    let org = EntityType::new("org");
    let topic = EntityType::new("topic");
    assert!(entity_types_compatible(&person, &person));
    assert!(entity_types_compatible(&topic, &person));
    assert!(entity_types_compatible(&person, &topic));
    assert!(!entity_types_compatible(&person, &org));
    assert!(entity_types_compatible(&EntityType::new("company"), &org));
    assert!(entity_types_compatible(
        &EntityType::new("something-else"),
        &person
    ));
}

#[tokio::test]
async fn complete_groups_share_one_overlay_and_duplicate_claims_are_idempotent() {
    let fixture = Fixture::new().await;
    let group = PromotionGroup {
        assertions: vec![fixture.raw.clone()],
        work_run_id: fixture.run.clone(),
        doc_node_id: fixture.lineage.node.base.id.clone(),
        revision_id: fixture.lineage.revision.base.id.clone(),
    };
    let prepared = prepare_assertion_groups(
        &fixture.storage,
        SPACE,
        &[group.clone(), group],
        "fixture",
        &PromotionOptions::default(),
    )
    .await
    .unwrap();
    assert_eq!(prepared.result.entities_created, 2);
    assert_eq!(prepared.result.assertions_recorded, 1);
    assert_eq!(prepared.result.rels_created, 1);
    fixture.no_live_effects().await;
}

#[tokio::test]
#[expect(
    clippy::cloned_ref_to_slice_refs,
    reason = "Retain the owned evidence fixture in the original obsolete-alias replacement oracle."
)]
async fn replacing_a_source_revision_cannot_retain_obsolete_aliases() {
    let fixture = Fixture::new().await;
    let mut base = BaseFields::new(
        RecordId::new_v4(TableName::Entity),
        SPACE.into(),
        "fixture".into(),
    );
    fixture.lineage.inherit(&mut base);
    for step in base
        .provenance
        .chain
        .iter_mut()
        .filter(|step| step.step == "grounded_promotion")
    {
        step.input = RecordId::new_v4(TableName::DocRevision).to_string();
    }
    let mut old = Entity::new(base, EntityType::new("person"), "Åsa".into());
    old.identity_keys = vec!["åsa".into()];
    old.aliases = vec!["Obsolete private alias".into()];
    EntityRepository::new(fixture.storage.clone())
        .create(old.clone())
        .await
        .unwrap();
    let prepared = fixture.prepare(&[fixture.raw.clone()]).await.unwrap();
    assert_eq!(prepared.result.entities_created, 2);
    assert!(prepared
        .plan
        .effects
        .iter()
        .filter_map(|effect| effect.after.as_ref())
        .all(|record| record.base().id != old.base.id));
}

struct FixedVerdicts(Vec<(usize, bool)>);
#[async_trait::async_trait]
impl AssertionSupportVerifier for FixedVerdicts {
    fn identity(&self) -> &str {
        "fixed-protocol-oracle-v1"
    }
    async fn verify(
        &self,
        _candidates: &[SupportCandidate],
    ) -> Result<Vec<SupportVerdict>, PipelineError> {
        Ok(self
            .0
            .iter()
            .map(|(index, supported)| SupportVerdict {
                index: *index,
                supported: *supported,
            })
            .collect())
    }
}

#[tokio::test]
async fn support_protocol_rejects_omissions_duplicates_and_foreign_indices() {
    let fixture = Fixture::new().await;
    for verdicts in [
        vec![],
        vec![(0, true), (0, true)],
        vec![(1, true)],
        vec![(0, true), (1, false)],
    ] {
        let mut raw = vec![fixture.raw.clone()];
        assert!(
            verify_assertion_support(&mut raw, TEXT, &FixedVerdicts(verdicts))
                .await
                .is_err()
        );
        assert!(
            raw[0].support_verification.is_none(),
            "failed re-verification must clear earlier approval"
        );
    }
}

#[tokio::test]
async fn invalid_or_oversized_evidence_never_reaches_support_verifier() {
    struct MustNotCall;
    #[async_trait::async_trait]
    impl AssertionSupportVerifier for MustNotCall {
        fn identity(&self) -> &str {
            "forbidden-call"
        }
        async fn verify(
            &self,
            _candidates: &[SupportCandidate],
        ) -> Result<Vec<SupportVerdict>, PipelineError> {
            panic!("invalid evidence was sent to a provider")
        }
    }
    let fixture = Fixture::new().await;
    let mut raw = fixture.raw.clone();
    raw.evidence_span.start = 1;
    verify_assertion_support(std::slice::from_mut(&mut raw), TEXT, &MustNotCall)
        .await
        .unwrap();
    assert!(raw.support_verification.is_none());
    let text = "x".repeat(10_001);
    raw.evidence_span.start = 0;
    raw.evidence_span.end = 10_001;
    raw.evidence_span.quote_hash = AssertionValidator::hash_quote(&text);
    raw.quote = Some(text.clone());
    verify_assertion_support(std::slice::from_mut(&mut raw), &text, &MustNotCall)
        .await
        .unwrap();
    assert!(raw.support_verification.is_none());
}

#[tokio::test]
async fn altered_entity_hint_does_not_inherit_an_existing_support_receipt() {
    let fixture = Fixture::new().await;
    let mut raw = fixture.raw.clone();
    raw.subject_entity_id = Some(RecordId::new_v4(TableName::Entity).to_string());
    assert!(fixture.prepare(&[raw]).await.is_err());
    fixture.no_live_effects().await;
}

#[tokio::test]
async fn typed_literal_roundtrip_keeps_date_and_number_distinct() {
    let fixture = Fixture::new().await;
    let mut raw = fixture.raw.clone();
    raw.object_literal = Some(Literal::Date("2024-01-15".into()));
    raw.object_text = None;
    // This test checks lossless serialization, not support. The unrelated
    // receipt must remain invalid after semantics change.
    let mut serialized =
        super::super::extraction_work_kinds::SerializedRawAssertion::from_raw(&raw).unwrap();
    let restored = serialized
        .to_raw(
            &fixture.lineage.revision.base.id,
            &fixture.lineage.node.base.id,
        )
        .unwrap();
    assert_eq!(restored.object_literal, raw.object_literal);
    assert!(restored.validate_support().is_err());
    serialized.object_literal = Some(serde_json::json!(17));
    assert!(serialized
        .to_raw(
            &fixture.lineage.revision.base.id,
            &fixture.lineage.node.base.id
        )
        .is_err());
}

#[tokio::test]
#[expect(
    clippy::cloned_ref_to_slice_refs,
    reason = "Retain the original owned evidence call and authorization-failure oracle without changing future storage."
)]
async fn disambiguator_authorization_failure_aborts_plan_instead_of_reusing() {
    #[derive(Debug)]
    struct Denied;
    #[async_trait::async_trait]
    impl PromotionDisambiguator for Denied {
        async fn disambiguate(
            &self,
            _input: &DisambiguationRequest,
        ) -> Result<Option<PromotionDisambiguationDecision>, PipelineError> {
            Err(PipelineError::ConfigError(
                "processing approval revoked".into(),
            ))
        }
    }
    let fixture = Fixture::new().await;
    for summary in ["First supported person", "Different supported person"] {
        let mut base = BaseFields::new(
            RecordId::new_v4(TableName::Entity),
            SPACE.into(),
            "fixture".into(),
        );
        fixture.lineage.inherit(&mut base);
        let mut entity = Entity::new(base, EntityType::new("person"), "Åsa".into());
        entity.identity_keys = vec!["åsa".into()];
        entity.summary = Some(summary.into());
        EntityRepository::new(fixture.storage.clone())
            .create(entity)
            .await
            .unwrap();
    }
    let result = prepare_assertions_inline_with_options(
        &fixture.storage,
        SPACE,
        &[fixture.raw.clone()],
        &fixture.run,
        &fixture.lineage.node.base.id,
        &fixture.lineage.revision.base.id,
        "fixture",
        &PromotionOptions {
            disambiguator: Some(Arc::new(Denied)),
            ..PromotionOptions::default()
        },
    )
    .await;
    assert!(result.is_err());
    assert!(AssertionRepository::new(fixture.storage.clone())
        .list(SPACE, 10, None)
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn support_receipt_binds_coordinates_and_checked_translation_preserves_it() {
    let fixture = Fixture::new().await;
    let mut moved = fixture.raw.clone();
    moved.evidence_span.start += 7;
    moved.evidence_span.end += 7;
    assert!(moved.validate_support().is_err());
    assert!(moved.translate_evidence_offset(2, 500).is_err());

    let mut translated = fixture.raw.clone();
    translated
        .translate_evidence_offset(7, TEXT.len() + 7)
        .unwrap();
    translated.validate_support().unwrap();
    let text = format!("prefix:{TEXT}");
    AssertionValidator::validate_evidence(&translated, &text).unwrap();
    assert_eq!(translated.evidence_span.start, 7);
    assert_eq!(
        translated.evidence_span.quote_hash,
        fixture.raw.evidence_span.quote_hash
    );
}

#[tokio::test]
async fn serialized_verifier_identity_cannot_be_changed_without_invalidating_support() {
    let fixture = Fixture::new().await;
    let mut raw = fixture.raw.clone();
    let mut receipt = serde_json::to_value(raw.support_verification.as_ref().unwrap()).unwrap();
    receipt["model"] = serde_json::json!("another-model-that-never-verified-this");
    raw.support_verification = Some(serde_json::from_value(receipt).unwrap());
    assert!(raw.validate_support().is_err());
}

#[tokio::test]
async fn claim_order_does_not_change_duplicate_selection_or_effect_counts() {
    let fixture = Fixture::new().await;
    let mut weaker = fixture.raw.clone();
    weaker.confidence = 0.42;
    verify_assertion_support(std::slice::from_mut(&mut weaker), TEXT, &FixtureVerifier)
        .await
        .unwrap();
    for claims in [
        vec![fixture.raw.clone(), weaker.clone()],
        vec![weaker, fixture.raw.clone()],
    ] {
        let prepared = fixture.prepare(&claims).await.unwrap();
        assert_eq!(prepared.result.entities_created, 2);
        assert_eq!(prepared.result.assertions_recorded, 1);
        assert_eq!(prepared.result.rels_created, 1);
        let assertions: Vec<_> = prepared
            .plan
            .effects
            .iter()
            .filter_map(|effect| match effect.after.as_ref() {
                Some(CanonicalRecord::Assertion(assertion)) => Some(assertion),
                _ => None,
            })
            .collect();
        assert_eq!(assertions.len(), 1);
        assert!((assertions[0].confidence - 0.97).abs() < f32::EPSILON);
    }
    fixture.no_live_effects().await;
}

#[tokio::test]
async fn promotion_phase_future_storage_is_bounded() {
    fn future_size<F, Fut>(_: F) -> usize
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future,
    {
        std::mem::size_of::<Fut>()
    }
    let fixture = Fixture::new().await;
    let options = PromotionOptions::default();
    // This plan is never polled or published: inspect future types without
    // constructing their potentially large values or manufacturing authority.
    let capture = PlanCapture::new(CanonicalPlan::default());
    let assertions = std::slice::from_ref(&fixture.raw);
    let group = future_size(|| {
        prepare_group(
            &fixture.storage,
            SPACE,
            assertions,
            &fixture.run,
            &fixture.lineage.node.base.id,
            &fixture.lineage.revision.base.id,
            "fixture",
            &options,
            &capture,
        )
    });
    let outer = future_size(|| fixture.prepare(assertions));
    for (phase, bytes) in [("promotion group", group), ("promotion wrapper", outer)] {
        assert!(
            bytes <= 128 * 1024,
            "{phase} future uses {bytes} bytes before finalizer frames"
        );
    }
}

/// Sanitized, metadata-only stand-in for an internal security-review document.
fn internal_review_metadata() -> serde_json::Value {
    serde_json::json!({
        HOST_PROJECTION_METADATA_KEY: HOST_PROJECTION_INTERNAL_REVIEW,
        "kind": "review_bookkeeping",
    })
}

fn planned_events(prepared: &PreparedPromotion) -> Vec<(Option<Event>, Event)> {
    prepared
        .plan
        .effects
        .iter()
        .filter_map(|effect| match effect.after.as_ref() {
            Some(CanonicalRecord::Event(after)) => Some((
                effect.before.as_ref().map(|before| match before {
                    CanonicalRecord::Event(before) => (**before).clone(),
                    _ => panic!("event effect has a foreign before-image"),
                }),
                (**after).clone(),
            )),
            _ => None,
        })
        .collect()
}

/// Commit only the planned event, as an earlier processing run would have.
async fn store_planned_event(fixture: &Fixture, status: EventStatus, deleted: bool) -> Event {
    let prepared = fixture
        .prepare(std::slice::from_ref(&fixture.raw))
        .await
        .unwrap();
    let (_, mut event) = planned_events(&prepared).pop().expect("planned event");
    event.status = status;
    // Events may only reference stored entities: the source's subject and the
    // participants the same run planned.
    let entities = EntityRepository::new(fixture.storage.clone());
    for subject in &fixture.lineage.source.base.subjects {
        let mut base = BaseFields::new(subject.id().clone(), SPACE.into(), "fixture".into());
        fixture.lineage.inherit(&mut base);
        entities
            .create(Entity::new(base, EntityType::new("person"), "Owner".into()))
            .await
            .unwrap();
    }
    for effect in &prepared.plan.effects {
        if let Some(CanonicalRecord::Entity(entity)) = effect.after.as_ref() {
            entities.create((**entity).clone()).await.unwrap();
        }
    }
    let event = EventRepository::new(fixture.storage.clone())
        .create(event)
        .await
        .unwrap();
    if deleted {
        EventRepository::new(fixture.storage.clone())
            .delete(SPACE, &event.base.id)
            .await
            .unwrap();
    }
    event
}

async fn mark_doc(fixture: &Fixture, metadata: serde_json::Value) {
    let mut doc = fixture.lineage.doc.clone();
    doc.metadata = metadata;
    DocRepository::new(fixture.storage.clone())
        .update(SPACE, &doc.base.id, doc.clone())
        .await
        .unwrap();
}

#[tokio::test]
async fn internal_review_doc_records_assertion_but_no_household_event() {
    let fixture = Fixture::with_doc_metadata(internal_review_metadata()).await;
    for _ in 0..2 {
        // Reprocessing the same document plans the same thing every time.
        let prepared = fixture
            .prepare(std::slice::from_ref(&fixture.raw))
            .await
            .unwrap();
        assert_eq!(prepared.result.assertions_recorded, 1);
        assert_eq!(prepared.result.rels_created, 1);
        assert_eq!(prepared.result.events_created, 0);
        assert!(planned_events(&prepared).is_empty());
        let assertions = prepared
            .plan
            .effects
            .iter()
            .filter(|effect| matches!(effect.after, Some(CanonicalRecord::Assertion(_))))
            .count();
        assert_eq!(assertions, 1, "history stays searchable");
    }
    fixture.no_live_effects().await;
}

#[tokio::test]
async fn internal_review_doc_never_refines_or_resurrects_an_earlier_event() {
    for (status, deleted) in [
        (EventStatus::Approved, false),
        (EventStatus::Rejected, false),
        (EventStatus::Approved, true),
    ] {
        let fixture = Fixture::new().await;
        let stored = store_planned_event(&fixture, status, deleted).await;
        mark_doc(&fixture, internal_review_metadata()).await;
        for _ in 0..2 {
            let prepared = fixture
                .prepare(std::slice::from_ref(&fixture.raw))
                .await
                .unwrap();
            assert_eq!(prepared.result.assertions_recorded, 1);
            assert_eq!(prepared.result.events_created, 0);
            assert!(
                planned_events(&prepared).is_empty(),
                "no create or refinement for {status:?} deleted={deleted}"
            );
        }
        let live = EventRepository::new(fixture.storage.clone())
            .get(SPACE, &stored.base.id)
            .await
            .unwrap();
        if deleted {
            assert!(live.is_none());
        } else {
            assert_eq!(live.map(|event| event.status), Some(status));
        }
    }
}

#[tokio::test]
async fn household_doc_still_creates_event_without_or_with_copied_projection() {
    for (metadata, projection) in [
        (serde_json::json!({}), None),
        (
            serde_json::json!({ HOST_PROJECTION_METADATA_KEY: null }),
            None,
        ),
        (
            serde_json::json!({ HOST_PROJECTION_METADATA_KEY: "household" }),
            Some(serde_json::json!("household")),
        ),
    ] {
        let fixture = Fixture::with_doc_metadata(metadata).await;
        let prepared = fixture
            .prepare(std::slice::from_ref(&fixture.raw))
            .await
            .unwrap();
        assert_eq!(prepared.result.events_created, 1);
        let events = planned_events(&prepared);
        assert_eq!(events.len(), 1);
        let (before, event) = &events[0];
        assert!(before.is_none());
        assert_eq!(event.metadata["origin"], "inline_promotion");
        assert_eq!(
            event.metadata.get(HOST_PROJECTION_METADATA_KEY).cloned(),
            projection
        );
        fixture.no_live_effects().await;
    }
}

#[tokio::test]
async fn operator_rejected_event_stays_rejected_after_reprocessing() {
    let fixture = Fixture::new().await;
    let rejected = store_planned_event(&fixture, EventStatus::Rejected, false).await;
    for _ in 0..2 {
        let prepared = fixture
            .prepare(std::slice::from_ref(&fixture.raw))
            .await
            .unwrap();
        assert_eq!(prepared.result.events_created, 0);
        let events = planned_events(&prepared);
        assert_eq!(events.len(), 1, "support is still refined");
        let (before, after) = &events[0];
        assert_eq!(
            before.as_ref().map(|event| &event.base.id),
            Some(&rejected.base.id)
        );
        assert_eq!(after.base.id, rejected.base.id);
        assert_eq!(after.status, EventStatus::Rejected);
        assert_eq!(
            after.assertion_refs.len(),
            rejected.assertion_refs.len() + 1
        );
    }
    assert_eq!(
        EventRepository::new(fixture.storage.clone())
            .get(SPACE, &rejected.base.id)
            .await
            .unwrap()
            .map(|event| event.status),
        Some(EventStatus::Rejected)
    );
}

#[tokio::test]
async fn temporal_refinement_preserves_rejection_but_still_promotes_candidates() {
    let raw = Fixture::new().await.raw;
    let base = BaseFields::new(
        RecordId::new_v4(TableName::Event),
        SPACE.into(),
        "fixture".into(),
    );
    for (status, expected) in [
        (EventStatus::Candidate, EventStatus::Approved),
        (EventStatus::Approved, EventStatus::Approved),
        (EventStatus::Rejected, EventStatus::Rejected),
    ] {
        let mut event = Event::new(
            base.clone(),
            "Åsa works at",
            vec![Link::new(RecordId::new_v4(TableName::Assertion))],
            vec![],
        )
        .unwrap();
        event.status = status;
        event.confidence = 0.5;
        assert!(apply_event_temporal_refinement(
            &mut event, &raw, None, 0.85
        ));
        assert_eq!(event.status, expected);
        assert!((event.confidence - 0.97).abs() < f32::EPSILON);
    }
}
