//! Real-backend maintenance preparation/rollback oracles. No provider calls.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use super::*;
use elephant_storage::canonical_commit::CanonicalEffect;
use elephant_storage::{EntityRepository, RelRepository, SpaceConfigRepository};
use elephant_types::records::base::Extractor;
use elephant_types::records::entity::EntityType;
use elephant_types::records::rel::ClaimStatus;
use elephant_types::Link;

#[path = "maintenance_audit_authority_tests.rs"]
mod audit_authority;

#[path = "maintenance_empty_slot_tests.rs"]
mod empty_slots;

const SPACE: &str = "maintenance-oracle";

#[test]
fn maintenance_commit_evidence_children_have_bounded_future_types() {
    fn bytes<A, F: std::future::Future>(_: impl FnOnce(A) -> F) -> usize {
        std::mem::size_of::<F>()
    }
    // Uncalled closures inspect the actual private future types without a
    // storage, fabricated reference, provider, runtime or future construction.
    // This guards state-machine aggregation, not cumulative poll-stack usage;
    // the real Definition cases still have to traverse the complete commit.
    let orchestration = [
        (
            "commit",
            bytes(
                |(planning, input): (
                    &'static PlanningTruthMaintenanceStorage,
                    &'static serde_json::Value,
                )| {
                    planning.commit("wiki_definition_plan", "scope", input, input)
                },
            ),
        ),
        (
            "prepare request",
            bytes(
                |(planning, input): (
                    &'static PlanningTruthMaintenanceStorage,
                    &'static serde_json::Value,
                )| {
                    planning.prepare_request("wiki_definition_plan", "scope", input, input)
                },
            ),
        ),
        (
            "published support",
            bytes(|planning: &'static PlanningTruthMaintenanceStorage| {
                planning.capture_published_support()
            }),
        ),
        (
            "decision evidence",
            bytes(|planning: &'static PlanningTruthMaintenanceStorage| {
                planning.validate_decision_evidence()
            }),
        ),
    ];
    for (phase, size) in orchestration {
        assert!(size <= 8 * 1024, "{phase} future is {size} bytes");
    }
    let evidence = [
        (
            "assertion",
            bytes(
                |(planning, row): (
                    &'static PlanningTruthMaintenanceStorage,
                    &'static Assertion,
                )| { planning.validate_assertion_evidence(row) },
            ),
        ),
        (
            "relationship",
            bytes(
                |(planning, row): (&'static PlanningTruthMaintenanceStorage, &'static Rel)| {
                    planning.validate_rel_evidence_at(row, false)
                },
            ),
        ),
        (
            "attribute",
            bytes(
                |(planning, row): (
                    &'static PlanningTruthMaintenanceStorage,
                    &'static Attribute,
                )| {
                    planning.validate_claim_evidence_at(&row.base, &row.evidence, false)
                },
            ),
        ),
        (
            "event",
            bytes(
                |(planning, row): (&'static PlanningTruthMaintenanceStorage, &'static Event)| {
                    planning.validate_event_evidence_at(row, false)
                },
            ),
        ),
    ];
    for (phase, size) in evidence {
        assert!(size <= 32 * 1024, "{phase} evidence future is {size} bytes");
    }
}

#[test]
fn request_identity_survives_native_object_order_without_losing_payload_changes() {
    let original: serde_json::Value = serde_json::from_str(
        r#"{"source":{"metadata":{"name":"Ada","contact":{"role":"engineer","company":"Research"}}},"inputs":[{"z":1,"a":2},3]}"#,
    )
    .unwrap();
    let reordered: serde_json::Value = serde_json::from_str(
        r#"{"inputs":[{"a":2,"z":1},3],"source":{"metadata":{"contact":{"company":"Research","role":"engineer"},"name":"Ada"}}}"#,
    )
    .unwrap();
    let fingerprint = request_fingerprint(&original).unwrap();
    assert_eq!(fingerprint, request_fingerprint(&reordered).unwrap());
    let mut changed = reordered.clone();
    changed["source"]["metadata"]["contact"]["company"] = serde_json::json!("Other");
    assert_ne!(fingerprint, request_fingerprint(&changed).unwrap());
    let mut changed = reordered.clone();
    changed["inputs"].as_array_mut().unwrap().reverse();
    assert_ne!(fingerprint, request_fingerprint(&changed).unwrap());
    let mut changed = reordered;
    changed["inputs"][1] = serde_json::json!("3");
    assert_ne!(fingerprint, request_fingerprint(&changed).unwrap());
}

#[tokio::test]
async fn contact_metadata_and_shared_entities_do_not_grant_structured_support() {
    for embedded in [true, false] {
        let client = setup(embedded).await;
        let mut source = Source::new(
            base(TableName::Source),
            SourceType::Manual,
            "contact".into(),
        );
        source.metadata = serde_json::json!({"contact":{"name":"Ada","company":"Research"}});
        let source = elephant_storage::SourceRepository::new(client.clone())
            .create(source)
            .await
            .unwrap();
        let rels = RelRepository::new(client.clone());
        let mut row = claim(&client).await;
        row.base.provenance.sources = vec![source.base.id];
        let row = rels.update(SPACE, &row.base.id.clone(), row).await.unwrap();
        let planning = PlanningTruthMaintenanceStorage::new(client.clone(), SPACE, None)
            .await
            .unwrap();
        let mut after = row.clone();
        after.status = ClaimStatus::Approved;
        assert!(planning
            .update_rel(SPACE, &row.base.id, after)
            .await
            .is_err());
        assert!(planning.prepared_plan().unwrap().effects.is_empty());
        assert_eq!(rels.get(SPACE, &row.base.id).await.unwrap(), Some(row));
    }
}

#[tokio::test]
async fn structured_contact_after_image_retains_fact_identity_and_physical_contribution() {
    let client = setup(true).await;
    let mut before = claim(&client).await;
    before.base.provenance.sources = vec![RecordId::new_v4(TableName::Source)];
    before.base.security.level = 2;
    let mut after = before.clone();
    after.status = ClaimStatus::Superseded;
    after.confidence = 0.2;
    after.base.deleted_at = Some(Utc::now());
    assert!(retains_contact_fact(&before, &after).unwrap());
    for changed in [
        {
            let mut row = after.clone();
            row.base.id = RecordId::new_v4(TableName::Rel);
            row
        },
        {
            let mut row = after.clone();
            row.object = Link::new(RecordId::new_v4(TableName::Entity));
            row
        },
        {
            let mut row = after.clone();
            row.rel_type = "invented".into();
            row
        },
        {
            let mut row = after.clone();
            row.base
                .provenance
                .sources
                .push(RecordId::new_v4(TableName::Source));
            row
        },
        {
            let mut row = after.clone();
            row.base.provenance.sources.clear();
            row
        },
        {
            let mut row = after.clone();
            row.base.security.level = 1;
            row
        },
        {
            let mut row = after;
            row.confidence = f32::NAN;
            row
        },
    ] {
        assert!(!retains_contact_fact(&before, &changed).unwrap());
    }
    // Shape preservation alone is not support: the previous test exercises
    // rejection at the actual storage-owned accepted-proof boundary.
}

#[tokio::test]
async fn discarded_preparation_branch_cannot_commit_its_effects_or_refresh_epoch() {
    for embedded in [true, false] {
        let client = setup(embedded).await;
        let row = claim(&client).await;
        let original = PlanningTruthMaintenanceStorage::new(client.clone(), SPACE, None)
            .await
            .unwrap();
        original.observe(&row).unwrap();
        let branch = original.fork().unwrap();
        let mut changed = row.clone();
        changed.status = ClaimStatus::Approved;
        branch.stage(Some(row.clone()), changed).unwrap();
        assert_eq!(branch.prepared_plan().unwrap().effects.len(), 1);
        assert!(original.prepared_plan().unwrap().effects.is_empty());
        assert_eq!(
            original.prepared_plan().unwrap().observed_epoch,
            branch.prepared_plan().unwrap().observed_epoch
        );
        drop(branch);
        assert_eq!(
            RelRepository::new(client.clone())
                .get(SPACE, &row.base.id)
                .await
                .unwrap(),
            Some(row.clone())
        );
        let mut newer = row.clone();
        newer.confidence = 0.1;
        RelRepository::new(client.clone())
            .update(SPACE, &row.base.id, newer)
            .await
            .unwrap();
        let mut obsolete = row.clone();
        obsolete.status = ClaimStatus::Superseded;
        original.stage(Some(row), obsolete).unwrap();
        assert!(original
            .commit(
                "resolution_test",
                "discarded-branch-stale",
                &serde_json::json!({}),
                &()
            )
            .await
            .is_err());
    }
}

#[tokio::test]
async fn inherited_context_only_tightens_authority_from_real_observed_rows() {
    let client = setup(true).await;
    let repo = EntityRepository::new(client.clone());
    let mut public_base = base(TableName::Entity);
    public_base.security.policy_id = "household".into();
    public_base.subjects_state = SubjectsState::Complete;
    let target = repo
        .create(Entity::new(
            public_base.clone(),
            EntityType::new("person"),
            "Ada".into(),
        ))
        .await
        .unwrap();
    let mut private_base = public_base;
    private_base.id = RecordId::new_v4(TableName::Entity);
    private_base.security.level = 2;
    private_base.security.labels = vec!["private".into()];
    private_base.subjects_state = SubjectsState::Partial;
    let context = repo
        .create(Entity::new(
            private_base,
            EntityType::new("person"),
            "Context".into(),
        ))
        .await
        .unwrap();
    let planning = PlanningTruthMaintenanceStorage::new(client, SPACE, None)
        .await
        .unwrap();
    let mut after = target.clone();
    elephant_storage::public_mutation::inherit_public_envelope(&mut after.base, &context.base)
        .unwrap();
    assert!(planning
        .stage_restricted(Some(target.clone()), after.clone())
        .is_err());
    assert!(planning
        .stage_inherited(
            Some(target.clone()),
            after.clone(),
            std::slice::from_ref(&context.base)
        )
        .is_err());
    planning.observe(&context).unwrap();
    planning
        .stage_inherited(
            Some(target.clone()),
            after.clone(),
            std::slice::from_ref(&context.base),
        )
        .unwrap();
    assert!(contains_authorization(&after.base, &target.base).unwrap());
    assert!(!contains_authorization(&target.base, &after.base).unwrap());
    let mut foreign = context.base.clone();
    foreign.security.policy_id = "another-policy".into();
    assert!(!contains_authorization(&after.base, &foreign).unwrap());
    let mut more_permissive = after.base.clone();
    more_permissive.subjects_state = SubjectsState::Complete;
    assert!(!contains_authorization(&more_permissive, &context.base).unwrap());
}

#[tokio::test]
async fn resolution_and_hygiene_notifications_reach_truth_maintenance() {
    let client = setup(true).await;
    let row = claim(&client).await;
    for (kind, triggers) in [
        ("reprocess_maintenance", true),
        ("run_maintenance_sweep", true),
        ("execute_tm_commit_with_storage", false),
    ] {
        let planning = prepare_approval(&client, &row).await;
        let request = planning
            .prepare_request(kind, "propagation", &serde_json::json!({}), &())
            .await
            .unwrap();
        let mut found = false;
        for event in request.plan.outbox {
            let (_, events) =
                crate::truth_maintenance::trigger_events_from_system_event(&event).unwrap();
            found |= !events.is_empty();
        }
        assert_eq!(found, triggers, "{kind}");
    }
}

#[tokio::test]
async fn old_evidence_uses_original_assertions_and_new_effects_require_planned_subjects() {
    for embedded in [true, false] {
        let client = setup(embedded).await;
        let row = claim(&client).await;
        let (rel, _) = legacy_claim(&client, row, "Ada works at Research").await;
        let assertions = elephant_storage::AssertionRepository::new(client.clone());
        let assertion = assertions
            .get(SPACE, rel.evidence[0].assertion.id())
            .await
            .unwrap()
            .unwrap();
        let mut restricted = assertion.clone();
        restricted.base.subjects.push(rel.subject.clone());
        let invalid = PlanningTruthMaintenanceStorage::new(client.clone(), SPACE, None)
            .await
            .unwrap();
        invalid
            .stage_assertion(assertion.clone(), restricted.clone())
            .unwrap();
        let mut unprotected_after = rel.clone();
        unprotected_after.status = ClaimStatus::Approved;
        invalid.stage(Some(rel.clone()), unprotected_after).unwrap();
        assert!(invalid
            .prepare_request(
                "resolution_test",
                "missing-new-subject",
                &serde_json::json!({}),
                &()
            )
            .await
            .is_err());
        assert_eq!(
            assertions.get(SPACE, &assertion.base.id).await.unwrap(),
            Some(assertion.clone())
        );
        let planning = PlanningTruthMaintenanceStorage::new(client.clone(), SPACE, None)
            .await
            .unwrap();
        planning.stage_assertion(assertion, restricted).unwrap();
        let mut after = rel.clone();
        after.base.subjects.push(rel.subject.clone());
        after.status = ClaimStatus::Approved;
        planning
            .stage_restricted(Some(rel.clone()), after.clone())
            .unwrap();
        planning
            .commit(
                "resolution_test",
                "original-and-prepared",
                &serde_json::json!({"claim":rel.base.id}),
                &(),
            )
            .await
            .unwrap();
        let stored = RelRepository::new(client)
            .get(SPACE, &rel.base.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.base.subjects, after.base.subjects);
        assert_eq!(stored.status, ClaimStatus::Approved);
        assert_eq!(stored.evidence, rel.evidence);
    }
}

#[tokio::test]
async fn retired_claims_keep_exact_original_evidence_while_replacements_use_new_scope() {
    for embedded in [true, false] {
        let client = setup(embedded).await;
        let row = claim(&client).await;
        let (rel, _) = legacy_claim(&client, row, "Ada works at Research").await;
        let assertions = elephant_storage::AssertionRepository::new(client.clone());
        let assertion = assertions
            .get(SPACE, rel.evidence[0].assertion.id())
            .await
            .unwrap()
            .unwrap();
        let mut restricted = assertion.clone();
        restricted.base.subjects.push(rel.subject.clone());
        let mut retired = rel.clone();
        retired.status = ClaimStatus::Superseded;
        retired.base.deleted_at = Some(Utc::now());
        retired.base.updated_at = Utc::now();
        let before_record = rel.clone().record();
        assert!(
            retirement_uses_original_evidence(Some(&before_record), &retired.clone().record())
                .unwrap()
        );
        assert!(!retirement_uses_original_evidence(None, &retired.clone().record()).unwrap());
        for changed in [
            {
                let mut row = retired.clone();
                row.confidence = 0.2;
                row
            },
            {
                let mut row = retired.clone();
                row.rel_type = "invented".into();
                row
            },
            {
                let mut row = retired.clone();
                row.evidence.clear();
                row
            },
            {
                let mut row = retired.clone();
                row.base.security.level = 2;
                row
            },
            {
                let mut row = retired.clone();
                row.base.provenance.sources.clear();
                row
            },
            {
                let mut row = retired.clone();
                row.base.deleted_at = None;
                row
            },
        ] {
            assert!(
                !retirement_uses_original_evidence(Some(&before_record), &changed.record())
                    .unwrap()
            );
        }
        // A changed payload cannot borrow old assertion scope by being marked retired.
        let epoch = CanonicalRepository::new(client.clone())
            .epoch(SPACE)
            .await
            .unwrap();
        for (variant, forged) in [
            {
                let mut row = retired.clone();
                row.confidence = 0.2;
                row
            },
            {
                let mut row = retired.clone();
                row.evidence[0].span.quote_hash = "changed quote".into();
                row
            },
            {
                let mut row = retired.clone();
                row.evidence[0].span.start += 1;
                row
            },
            {
                let mut row = retired.clone();
                row.base.subjects.push(row.object.clone());
                row
            },
            {
                let mut row = retired.clone();
                row.base.security.policy_id = "foreign".into();
                row
            },
            {
                let mut row = retired.clone();
                row.subject = row.object.clone();
                row
            },
            {
                let mut row = retired.clone();
                row.evidence[0].assertion = Link::new(RecordId::new_v4(TableName::Assertion));
                row
            },
        ]
        .into_iter()
        .enumerate()
        {
            let invalid = PlanningTruthMaintenanceStorage::new(client.clone(), SPACE, None)
                .await
                .unwrap();
            invalid
                .stage_assertion(assertion.clone(), restricted.clone())
                .unwrap();
            let refused = match invalid.stage(Some(rel.clone()), forged) {
                Err(_) => true,
                Ok(_) => invalid
                    .commit(
                        "resolution_test",
                        "changed-retirement",
                        &serde_json::json!({"variant":variant}),
                        &(),
                    )
                    .await
                    .is_err(),
            };
            assert!(refused, "changed retirement variant {variant}");
            assert_eq!(
                assertions.get(SPACE, &assertion.base.id).await.unwrap(),
                Some(assertion.clone())
            );
            assert_eq!(
                RelRepository::new(client.clone())
                    .get(SPACE, &rel.base.id)
                    .await
                    .unwrap(),
                Some(rel.clone())
            );
            assert_eq!(
                CanonicalRepository::new(client.clone())
                    .epoch(SPACE)
                    .await
                    .unwrap(),
                epoch
            );
        }

        let planning = PlanningTruthMaintenanceStorage::new(client.clone(), SPACE, None)
            .await
            .unwrap();
        planning
            .stage_assertion(assertion.clone(), restricted.clone())
            .unwrap();
        planning.stage(Some(rel.clone()), retired).unwrap();
        let mut replacement = rel.clone();
        replacement.base.id = RecordId::new_v4(TableName::Rel);
        replacement.base.subjects.push(rel.subject.clone());
        planning.stage(None, replacement.clone()).unwrap();
        planning
            .commit(
                "resolution_test",
                "exact-retirement",
                &serde_json::json!({"claim":rel.base.id}),
                &(),
            )
            .await
            .unwrap();
        let stored = assertions
            .get(SPACE, &assertion.base.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.base.subjects, restricted.base.subjects);
        let stored = RelRepository::new(client)
            .get(SPACE, &replacement.base.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.base.subjects, replacement.base.subjects);
        assert_eq!(stored.evidence, replacement.evidence);
    }
}

#[tokio::test]
async fn classification_repairs_only_envelopes_bound_to_complete_actual_source_evidence() {
    for embedded in [true, false] {
        let client = setup(embedded).await;
        let original = claim(&client).await;
        let (mut rel, mut source) = legacy_claim(&client, original, "Ada works at Research").await;
        let assertions = elephant_storage::AssertionRepository::new(client.clone());
        let mut assertion = assertions
            .get(SPACE, rel.evidence[0].assertion.id())
            .await
            .unwrap()
            .unwrap();
        let revisions = elephant_storage::DocRevisionRepository::new(client.clone());
        let mut revision = revisions
            .get(SPACE, assertion.evidence.revision.id())
            .await
            .unwrap()
            .unwrap();
        let docs = elephant_storage::DocRepository::new(client.clone());
        let mut doc = docs.get(SPACE, revision.doc.id()).await.unwrap().unwrap();
        let private = Security {
            policy_id: "household".into(),
            level: 2,
            labels: vec!["private".into()],
            handling: vec![],
        };
        source.base.security = private.clone();
        elephant_storage::SourceRepository::new(client.clone())
            .update(SPACE, &source.base.id, source.clone())
            .await
            .unwrap();
        revision.base.security = private.clone();
        revisions
            .update(SPACE, &revision.base.id, revision.clone())
            .await
            .unwrap();
        doc.base.security = private.clone();
        let doc = docs.update(SPACE, &doc.base.id, doc.clone()).await.unwrap();
        assertion.base.security.policy_id = "default".into();
        let assertion = assertions
            .update(SPACE, &assertion.base.id, assertion.clone())
            .await
            .unwrap();
        rel.base.security.policy_id = "default".into();
        let rels = RelRepository::new(client.clone());
        let rel = rels.update(SPACE, &rel.base.id, rel.clone()).await.unwrap();
        let planning = PlanningTruthMaintenanceStorage::new(client.clone(), SPACE, None)
            .await
            .unwrap();
        let mut repaired_assertion = assertion.clone();
        repaired_assertion.base.security = private.clone();
        let mut invented = repaired_assertion.clone();
        invented.subject_text = "Another person".into();
        assert!(planning
            .stage_source_classification(assertion.clone(), invented, std::slice::from_ref(&doc))
            .is_err());
        assert!(planning.prepared_plan().unwrap().effects.is_empty());
        planning
            .stage_source_classification(
                assertion.clone(),
                repaired_assertion,
                std::slice::from_ref(&doc),
            )
            .unwrap();
        let mut repaired_rel = rel.clone();
        repaired_rel.base.security = private.clone();
        planning
            .stage_source_classification(rel.clone(), repaired_rel, std::slice::from_ref(&doc))
            .unwrap();
        assert_eq!(
            rels.get(SPACE, &rel.base.id).await.unwrap(),
            Some(rel.clone())
        );
        planning
            .commit(
                "classification_backfill",
                "actual-source",
                &serde_json::json!({"source":source.base.id}),
                &(),
            )
            .await
            .unwrap();
        let stored = rels.get(SPACE, &rel.base.id).await.unwrap().unwrap();
        assert_eq!(stored.base.security, private);
        assert_eq!(stored.evidence, rel.evidence);
        assert_eq!(stored.status, rel.status);
        let stored_assertion = assertions
            .get(SPACE, &assertion.base.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored_assertion.evidence, assertion.evidence);
        assert_eq!(stored_assertion.subject_text, assertion.subject_text);
    }
}

#[tokio::test]
async fn classification_cannot_borrow_an_unrelated_document_to_relabel_intrinsic_claim() {
    let client = setup(true).await;
    let mut rel = claim(&client).await;
    rel.base.security.policy_id = "default".into();
    let rel = RelRepository::new(client.clone())
        .update(SPACE, &rel.base.id, rel.clone())
        .await
        .unwrap();
    let mut doc_base = base(TableName::Doc);
    doc_base.security = Security {
        policy_id: "household".into(),
        level: 1,
        labels: vec!["private".into()],
        handling: vec![],
    };
    let doc = elephant_storage::DocRepository::new(client.clone())
        .create(Doc {
            base: doc_base,
            title: "Unrelated".into(),
            text: "Unrelated".into(),
            sidecar_text: None,
            source: None,
            media_attachments: vec![],
            rendition_key: "unrelated".into(),
            metadata: serde_json::Value::Null,
        })
        .await
        .unwrap();
    let planning = PlanningTruthMaintenanceStorage::new(client.clone(), SPACE, None)
        .await
        .unwrap();
    let mut after = rel.clone();
    after.base.security = doc.base.security.clone();
    planning
        .stage_source_classification(rel.clone(), after, &[doc])
        .unwrap();
    assert!(planning
        .prepare_request(
            "classification_backfill",
            "unrelated",
            &serde_json::json!({}),
            &()
        )
        .await
        .is_err());
    assert_eq!(
        RelRepository::new(client)
            .get(SPACE, &rel.base.id)
            .await
            .unwrap(),
        Some(rel)
    );
}

#[tokio::test]
async fn subplan_adoption_keeps_original_epoch_dependencies_and_before_images() {
    let client = setup(true).await;
    let row = claim(&client).await;
    let planning = PlanningTruthMaintenanceStorage::new(client, SPACE, None)
        .await
        .unwrap();
    let original = planning.prepared_plan().unwrap();
    let mut wrong_epoch = original.clone();
    wrong_epoch.observed_epoch.push_str("-stale");
    assert!(planning.adopt_plan(&original, wrong_epoch).is_err());
    assert!(planning.prepared_plan().unwrap().effects.is_empty());
    planning.observe(&row).unwrap();
    let mut updated = row.clone();
    updated.status = ClaimStatus::Approved;
    planning.stage(Some(row.clone()), updated.clone()).unwrap();
    let snapshot = planning.prepared_plan().unwrap();
    let mut dropped = snapshot.clone();
    dropped.effects.clear();
    assert!(planning.adopt_plan(&snapshot, dropped).is_err());
    let mut lost_dependency = snapshot.clone();
    lost_dependency.dependencies.clear();
    assert!(planning.adopt_plan(&snapshot, lost_dependency).is_err());
    let mut wrong_before = snapshot.clone();
    wrong_before.effects[0].before = Some(updated.clone().record());
    assert!(planning.adopt_plan(&snapshot, wrong_before).is_err());
    assert!(same(&snapshot, &planning.prepared_plan().unwrap()).unwrap());
    let mut sequential = snapshot.clone();
    let mut after = updated;
    after.confidence = 0.75;
    sequential.effects[0].after = Some(after.clone().record());
    assert!(planning.adopt_plan(&original, sequential.clone()).is_err());
    planning.adopt_plan(&snapshot, sequential).unwrap();
    let joined = planning.prepared_plan().unwrap();
    assert_eq!(joined.effects.len(), 1);
    assert!(same(joined.effects[0].before.as_ref().unwrap(), &row.record()).unwrap());
    assert!(same(joined.effects[0].after.as_ref().unwrap(), &after.record()).unwrap());
}

#[tokio::test]
async fn derived_subject_requirements_preserve_real_source_evidence_authority() {
    for embedded in [true, false] {
        let client = setup(embedded).await;
        let original = claim(&client).await;
        let (mut rel, _) = legacy_claim(&client, original, "Ada works at Research").await;
        let assertions = elephant_storage::AssertionRepository::new(client.clone());
        let id = rel.evidence[0].assertion.id();
        let mut assertion = assertions.get(SPACE, id).await.unwrap().unwrap();
        assertion.base.subjects = vec![rel.subject.clone()];
        assertions
            .update(SPACE, id, assertion.clone())
            .await
            .unwrap();
        rel.base.subjects = vec![rel.subject.clone(), rel.object.clone()];
        let rel = RelRepository::new(client.clone())
            .update(SPACE, &rel.base.id, rel.clone())
            .await
            .unwrap();
        let planning = PlanningTruthMaintenanceStorage::new(client.clone(), SPACE, None)
            .await
            .unwrap();
        planning
            .validate_assertion_evidence(&assertion)
            .await
            .unwrap();
        let mut approved = rel.clone();
        approved.status = ClaimStatus::Approved;
        planning
            .update_rel(SPACE, &rel.base.id, approved)
            .await
            .unwrap();
        planning
            .commit(
                "subject_containment",
                "real-source",
                &serde_json::json!({"claim":rel.base.id}),
                &(),
            )
            .await
            .unwrap();
        assert_eq!(
            RelRepository::new(client.clone())
                .get(SPACE, &rel.base.id)
                .await
                .unwrap()
                .unwrap()
                .status,
            ClaimStatus::Approved
        );
        let mut lacking_subject = rel.base.clone();
        lacking_subject.subjects.clear();
        assert!(!contains_authorization(&lacking_subject, &assertion.base).unwrap());
        let mut changed_policy = rel.base.clone();
        changed_policy.security.policy_id = "unrelated-policy".into();
        assert!(!contains_authorization(&changed_policy, &assertion.base).unwrap());
    }
}

#[tokio::test]
async fn explicit_subject_restriction_cannot_remove_requirements_or_change_completeness() {
    let client = setup(true).await;
    let original = claim(&client).await;
    let planning = PlanningTruthMaintenanceStorage::new(client, SPACE, None)
        .await
        .unwrap();
    let mut restricted = original.clone();
    restricted.base.subjects = vec![restricted.subject.clone()];
    assert!(planning
        .stage(Some(original.clone()), restricted.clone())
        .is_err());
    planning
        .stage_restricted(Some(original.clone()), restricted.clone())
        .unwrap();
    assert!(planning
        .stage_restricted(Some(restricted.clone()), original)
        .is_err());
    let mut changed = restricted.clone();
    changed.base.subjects_state = elephant_types::records::base::SubjectsState::Complete;
    assert!(planning
        .stage_restricted(Some(restricted), changed)
        .is_err());
}

async fn setup(embedded: bool) -> StorageClient {
    let client = if embedded {
        StorageClient::new_embedded().await.unwrap()
    } else {
        let client = StorageClient::new_memory().await.unwrap();
        elephant_migrations::apply_schema(&client).await.unwrap();
        client
    };
    SpaceConfigRepository::new(client.clone())
        .create(SpaceConfig {
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
    client
}

fn base(table: TableName) -> BaseFields {
    BaseFields::new(RecordId::new_v4(table), SPACE.to_owned(), "test".to_owned())
}

async fn claim(client: &StorageClient) -> Rel {
    let repo = EntityRepository::new(client.clone());
    let subject = repo
        .create(Entity::new(
            base(TableName::Entity),
            EntityType::new("person"),
            "Ada".into(),
        ))
        .await
        .unwrap();
    let object = repo
        .create(Entity::new(
            base(TableName::Entity),
            EntityType::new("organization"),
            "Research".into(),
        ))
        .await
        .unwrap();
    RelRepository::new(client.clone())
        .create(Rel {
            base: base(TableName::Rel),
            subject: Link::new(subject.base.id),
            object: Link::new(object.base.id),
            rel_type: "works_at".into(),
            status: ClaimStatus::Candidate,
            confidence: 0.9,
            validity_window: None,
            evidence: vec![],
            temporal_audit: None,
        })
        .await
        .unwrap()
}

async fn prepare_approval(client: &StorageClient, before: &Rel) -> PlanningTruthMaintenanceStorage {
    let planning = PlanningTruthMaintenanceStorage::new(client.clone(), SPACE, None)
        .await
        .unwrap();
    let mut after = before.clone();
    after.status = ClaimStatus::Approved;
    planning
        .update_rel(SPACE, &before.base.id, after)
        .await
        .unwrap();
    planning
        .create_audit_log(AuditLog {
            base: base(TableName::AuditLog),
            action: AuditAction::Update,
            actor: "test".into(),
            record: before.base.id.clone(),
            before: None,
            after: None,
            timestamp: Utc::now(),
            reason: None,
        })
        .await
        .unwrap();
    planning
}

#[tokio::test]
async fn preparation_has_no_live_effects_and_coalesces_updates() {
    let client = setup(true).await;
    let before = claim(&client).await;
    let planning = prepare_approval(&client, &before).await;
    let mut after = planning
        .get_rel(SPACE, &before.base.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(after.status, ClaimStatus::Approved);
    after.confidence = 0.95;
    planning
        .update_rel(SPACE, &before.base.id, after)
        .await
        .unwrap();
    let plan = planning.prepared_plan().unwrap();
    assert_eq!(
        plan.effects
            .iter()
            .filter(|effect| matches!(&effect.after, Some(CanonicalRecord::Rel(_))))
            .count(),
        1
    );
    assert_eq!(
        RelRepository::new(client.clone())
            .get(SPACE, &before.base.id)
            .await
            .unwrap(),
        Some(before)
    );
    assert!(elephant_storage::AuditLogRepository::new(client)
        .list(SPACE, 20, None)
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn late_transaction_failure_rolls_back_claim_audit_outbox_and_receipt_on_both_backends() {
    for embedded in [true, false] {
        let client = setup(embedded).await;
        let before = claim(&client).await;
        let collision = EntityRepository::new(client.clone())
            .create(Entity::new(
                base(TableName::Entity),
                EntityType::new("topic"),
                "Already present".into(),
            ))
            .await
            .unwrap();
        let planning = prepare_approval(&client, &before).await;
        let mut request = planning
            .prepare_request(
                "resolution_test",
                "late-fault",
                &serde_json::json!({"v": 1}),
                &(),
            )
            .await
            .unwrap();
        let key = request.commit_id.clone();
        // This is a failure in the real transaction after earlier effects,
        // not a fake repository that returns an error before writing anything.
        request.plan.effects.push(CanonicalEffect {
            before: None,
            after: Some(CanonicalRecord::Entity(Box::new(collision))),
        });
        assert!(CanonicalRepository::new(client.clone())
            .commit(request)
            .await
            .is_err());
        assert_eq!(
            RelRepository::new(client.clone())
                .get(SPACE, &before.base.id)
                .await
                .unwrap(),
            Some(before)
        );
        assert!(elephant_storage::AuditLogRepository::new(client.clone())
            .list(SPACE, 20, None)
            .await
            .unwrap()
            .is_empty());
        assert!(elephant_storage::SystemEventRepository::new(client.clone())
            .list(SPACE, 20, None)
            .await
            .unwrap()
            .is_empty());
        assert!(CanonicalRepository::new(client)
            .receipt(SPACE, &key)
            .await
            .unwrap()
            .is_none());
    }
}

#[tokio::test]
async fn complete_receipt_replays_pending_successors_and_rejects_changed_payload() {
    let client = setup(true).await;
    let before = claim(&client).await;
    let planning = prepare_approval(&client, &before).await;
    let input = serde_json::json!({"input": "original"});
    let output = serde_json::json!({"pending_conflict_groups": [RecordId::new_v4(TableName::ConflictGroup)]});
    planning
        .commit("resolution_test", "replay", &input, &output)
        .await
        .unwrap();
    let retry = PlanningTruthMaintenanceStorage::new(client.clone(), SPACE, None)
        .await
        .unwrap();
    let replay: serde_json::Value = retry
        .replay("resolution_test", "replay", &input)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        replay, output,
        "commit-before-enqueue recovery must retain its work"
    );
    assert!(retry
        .replay::<serde_json::Value>(
            "resolution_test",
            "replay",
            &serde_json::json!({"input": "changed"})
        )
        .await
        .is_err());
    assert_eq!(
        elephant_storage::AuditLogRepository::new(client)
            .list(SPACE, 20, None)
            .await
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test]
async fn configuration_change_after_capture_refuses_entire_plan() {
    for embedded in [true, false] {
        let client = setup(embedded).await;
        let before = claim(&client).await;
        let planning = prepare_approval(&client, &before).await;
        let repo = SpaceConfigRepository::new(client.clone());
        let mut config = repo.get_by_space_id(SPACE).await.unwrap().unwrap();
        config.purpose = Some("new processing policy".into());
        let id = config.base.id.clone();
        repo.update(SPACE, &id, config).await.unwrap();
        assert!(planning
            .commit(
                "resolution_test",
                "stale-config",
                &serde_json::json!({}),
                &()
            )
            .await
            .is_err());
        assert_eq!(
            RelRepository::new(client)
                .get(SPACE, &before.base.id)
                .await
                .unwrap(),
            Some(before)
        );
    }
}

#[tokio::test]
async fn source_change_after_capture_refuses_even_source_less_effects() {
    let client = setup(true).await;
    let source_repo = elephant_storage::SourceRepository::new(client.clone());
    let source = source_repo
        .create(Source::new(
            base(TableName::Source),
            elephant_types::records::source::SourceType::Manual,
            "first".into(),
        ))
        .await
        .unwrap();
    let before = claim(&client).await;
    let planning = prepare_approval(&client, &before).await;
    source_repo.delete(SPACE, &source.base.id).await.unwrap();
    assert!(planning
        .commit(
            "resolution_test",
            "source-race",
            &serde_json::json!({}),
            &()
        )
        .await
        .is_err());
    assert_eq!(
        RelRepository::new(client)
            .get(SPACE, &before.base.id)
            .await
            .unwrap(),
        Some(before)
    );
}

#[tokio::test]
async fn fabricated_or_stale_attempt_cannot_commit_source_less_maintenance() {
    let client = setup(true).await;
    let before = claim(&client).await;
    let planning = PlanningTruthMaintenanceStorage::new(
        client.clone(),
        SPACE,
        Some(AttemptFence {
            space_id: SPACE.into(),
            work_item_id: RecordId::new_v4(TableName::WorkItem),
            generation: 999,
            lease_token: "not-current".into(),
        }),
    )
    .await
    .unwrap();
    let mut after = before.clone();
    after.status = ClaimStatus::Approved;
    planning
        .update_rel(SPACE, &before.base.id, after)
        .await
        .unwrap();
    assert!(planning
        .commit(
            "resolution_test",
            "stale-attempt",
            &serde_json::json!({}),
            &()
        )
        .await
        .is_err());
    assert_eq!(
        RelRepository::new(client)
            .get(SPACE, &before.base.id)
            .await
            .unwrap(),
        Some(before)
    );
}

#[tokio::test]
async fn original_claim_cancelled_after_preparation_cannot_publish() {
    let client = setup(true).await;
    let before = claim(&client).await;
    let repo = elephant_storage::WorkItemRepository::new(client.clone());
    let item = repo
        .create(WorkItem {
            base: base(TableName::WorkItem),
            kind: "tm_commit".into(),
            payload: serde_json::json!({}),
            status: WorkItemStatus::Runnable,
            priority: 1,
            idempotency_key: "cancelled-maintenance".into(),
            attempts: 0,
            lease_expires_at: None,
            leased_by: None,
            run_after: None,
            last_error: None,
        })
        .await
        .unwrap();
    let claimed = repo
        .claim_with_lease(
            SPACE,
            &item.base.id,
            "oracle",
            Utc::now() + chrono::Duration::minutes(5),
        )
        .await
        .unwrap()
        .unwrap();
    let fence = elephant_storage::work_attempt_repository::fence_from_claim(&claimed).unwrap();
    let planning = PlanningTruthMaintenanceStorage::new(client.clone(), SPACE, Some(fence.clone()))
        .await
        .unwrap();
    let mut after = before.clone();
    after.status = ClaimStatus::Approved;
    planning
        .update_rel(SPACE, &before.base.id, after)
        .await
        .unwrap();
    repo.update_running_for_worker(
        SPACE,
        &item.base.id,
        &fence.lease_token,
        WorkItemStatus::Cancelled,
        None,
    )
    .await
    .unwrap()
    .unwrap();
    assert!(planning
        .commit(
            "resolution_test",
            "cancelled-attempt",
            &serde_json::json!({}),
            &()
        )
        .await
        .is_err());
    assert_eq!(
        RelRepository::new(client)
            .get(SPACE, &before.base.id)
            .await
            .unwrap(),
        Some(before)
    );
}

#[tokio::test]
async fn shared_entity_does_not_grant_missing_source_authority() {
    let client = setup(true).await;
    let mut first = claim(&client).await;
    let mut second = first.clone();
    second.base.id = RecordId::new_v4(TableName::Rel);
    first.base.provenance.sources = vec![RecordId::new_v4(TableName::Source)];
    second.base.provenance.sources = vec![RecordId::new_v4(TableName::Source)];
    let repo = RelRepository::new(client.clone());
    let first = repo
        .update(SPACE, &first.base.id, first.clone())
        .await
        .unwrap();
    let second = repo.create(second).await.unwrap();
    let planning = PlanningTruthMaintenanceStorage::new(client.clone(), SPACE, None)
        .await
        .unwrap();
    let mut inherited = base(TableName::TruthSlot);
    planning
        .inherit_claims(
            &mut inherited,
            &[first.base.id.to_string(), second.base.id.to_string()],
        )
        .await
        .unwrap();
    assert_eq!(inherited.provenance.sources.len(), 2);
    assert!(planning
        .prepare_request(
            "resolution_test",
            "missing-support",
            &serde_json::json!({}),
            &()
        )
        .await
        .is_err());
    assert!(planning.prepared_plan().unwrap().effects.is_empty());
    assert_eq!(repo.get(SPACE, &first.base.id).await.unwrap(), Some(first));
    assert_eq!(
        repo.get(SPACE, &second.base.id).await.unwrap(),
        Some(second)
    );
}

#[tokio::test]
async fn source_less_baseline_survives_failed_source_contribution() {
    let client = setup(true).await;
    let before = claim(&client).await;
    let planning = PlanningTruthMaintenanceStorage::new(client.clone(), SPACE, None)
        .await
        .unwrap();
    let mut changed = before.clone();
    changed
        .base
        .provenance
        .sources
        .push(RecordId::new_v4(TableName::Source));
    assert!(planning
        .update_rel(SPACE, &before.base.id, changed)
        .await
        .is_err());
    assert_eq!(
        RelRepository::new(client)
            .get(SPACE, &before.base.id)
            .await
            .unwrap(),
        Some(before)
    );
}

async fn manual_assertion(client: &StorageClient) -> (Assertion, Entity) {
    let entity = EntityRepository::new(client.clone())
        .create(Entity::new(
            base(TableName::Entity),
            EntityType::new("person"),
            "Ada".into(),
        ))
        .await
        .unwrap();
    let now = Utc::now();
    let assertion = Assertion {
        base: base(TableName::Assertion),
        subject_text: "Ada".into(),
        predicate_raw: "role".into(),
        predicate_id: None,
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
        observed_at: now,
        asserted_at: now,
        assertion_key: uuid::Uuid::new_v4().to_string(),
        resolution_status: ResolutionStatus::Pending,
        resolution_method: None,
        resolution_confidence: None,
        retry_state: AssertionRetryState::default(),
        subject_entity: None,
        object_entity: None,
        confidence: 1.0,
        subject_entity_type: None,
        object_entity_type: None,
        significance: None,
        lifecycle_tier: "active".into(),
    };
    let assertion = elephant_storage::AssertionRepository::new(client.clone())
        .create(assertion)
        .await
        .unwrap();
    (assertion, entity)
}

fn patch(assertion: &Assertion, entity: &Entity) -> crate::resolution::ResolutionPatch {
    crate::resolution::ResolutionPatch {
        prepared_epoch: None,
        assertion_id: assertion.base.id.clone(),
        resolved_subject: Some(entity.base.id.clone()),
        resolved_object: None,
        confidence: 0.9,
        resolution_method: "manual".into(),
        status: ResolutionStatus::Resolved,
        abstention_reason: None,
    }
}

#[tokio::test]
async fn active_resolution_batch_never_persists_its_valid_prefix() {
    let client = setup(true).await;
    let (assertion, entity) = manual_assertion(&client).await;
    let first = patch(&assertion, &entity);
    let mut missing = first.clone();
    missing.assertion_id = RecordId::new_v4(TableName::Assertion);
    assert!(crate::resolution::apply_resolution_patches_with_attempt(
        &client,
        SPACE,
        "batch",
        &[first, missing],
        &crate::resolution::BackoffConfig::default(),
        None,
        None
    )
    .await
    .is_err());
    assert_eq!(
        elephant_storage::AssertionRepository::new(client.clone())
            .get(SPACE, &assertion.base.id)
            .await
            .unwrap(),
        Some(assertion)
    );
    assert!(elephant_storage::AuditLogRepository::new(client.clone())
        .list(SPACE, 20, None)
        .await
        .unwrap()
        .is_empty());
    assert!(elephant_storage::SystemEventRepository::new(client)
        .list(SPACE, 20, None)
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn active_resolution_replay_is_complete_and_changed_payload_conflicts() {
    for embedded in [true, false] {
        let client = setup(embedded).await;
        let (assertion, entity) = manual_assertion(&client).await;
        let original = patch(&assertion, &entity);
        for _ in 0..2 {
            crate::resolution::apply_resolution_patches_with_attempt(
                &client,
                SPACE,
                "operation",
                std::slice::from_ref(&original),
                &crate::resolution::BackoffConfig::default(),
                None,
                None,
            )
            .await
            .unwrap();
        }
        let mut changed = original;
        changed.confidence = 0.5;
        assert!(crate::resolution::apply_resolution_patches_with_attempt(
            &client,
            SPACE,
            "operation",
            &[changed],
            &crate::resolution::BackoffConfig::default(),
            None,
            None
        )
        .await
        .is_err());
        let row = elephant_storage::AssertionRepository::new(client.clone())
            .get(SPACE, &assertion.base.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(row.resolution_status, ResolutionStatus::Resolved);
        assert_eq!(row.resolution_confidence, Some(0.9));
        assert_eq!(
            elephant_storage::AuditLogRepository::new(client.clone())
                .list(SPACE, 20, None)
                .await
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            elephant_storage::SystemEventRepository::new(client)
                .list(SPACE, 20, None)
                .await
                .unwrap()
                .len(),
            1
        );
    }
}

/// Retained no-head sources exercise the explicit Legacy snapshot authority;
/// these are not substitutes for newly accepted whole-source publication tests.
async fn legacy_claim(client: &StorageClient, mut rel: Rel, text: &str) -> (Rel, Source) {
    let source = elephant_storage::SourceRepository::new(client.clone())
        .create(Source::new(
            base(TableName::Source),
            SourceType::Manual,
            crate::extraction::AssertionValidator::hash_quote(text),
        ))
        .await
        .unwrap();
    let mut doc_base = base(TableName::Doc);
    doc_base.provenance.sources = vec![source.base.id.clone()];
    let doc = elephant_storage::DocRepository::new(client.clone())
        .create(Doc {
            base: doc_base,
            title: "retained evidence".into(),
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
    let hash = crate::extraction::AssertionValidator::hash_quote(text);
    let revision = elephant_storage::DocRevisionRepository::new(client.clone())
        .create(DocRevision {
            base: revision_base,
            doc: Link::new(doc.base.id),
            text: text.into(),
            text_hash: hash.clone(),
            renderer: elephant_types::records::doc_revision::RendererMetadata {
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
    let (mut assertion, _) = manual_assertion(client).await;
    assertion.base.provenance.sources = vec![source.base.id.clone()];
    assertion.evidence = EvidenceSpan {
        revision: Link::new(revision.base.id),
        node: None,
        start: 0,
        end: u32::try_from(text.len()).unwrap(),
        quote_hash: hash,
    };
    elephant_storage::AssertionRepository::new(client.clone())
        .update(SPACE, &assertion.base.id, assertion.clone())
        .await
        .unwrap();
    rel.base.provenance.sources = vec![source.base.id.clone()];
    rel.evidence = vec![EvidenceRef {
        assertion: Link::new(assertion.base.id),
        span: assertion.evidence,
        time_basis: TimeBasis::ObservedAt,
        time_value: Utc::now(),
    }];
    let repo = RelRepository::new(client.clone());
    let rel = if repo.get(SPACE, &rel.base.id).await.unwrap().is_some() {
        repo.update(SPACE, &rel.base.id, rel.clone()).await.unwrap()
    } else {
        repo.create(rel).await.unwrap()
    };
    (rel, source)
}

#[tokio::test]
async fn cross_source_corroboration_records_an_inverse_preserving_independent_fields() {
    for embedded in [true, false] {
        let client = setup(embedded).await;
        let initial = claim(&client).await;
        let (first, _) = legacy_claim(&client, initial, "Ada works at Research").await;
        let mut other = first.clone();
        other.base.id = RecordId::new_v4(TableName::Rel);
        let (second, source_b) = legacy_claim(&client, other, "Research employs Ada").await;
        let planning = PlanningTruthMaintenanceStorage::new(client.clone(), SPACE, None)
            .await
            .unwrap();
        let mut merged = first.clone();
        merged.status = ClaimStatus::Approved;
        merged.evidence.extend(second.evidence.iter().cloned());
        planning
            .update_rel(SPACE, &first.base.id, merged)
            .await
            .unwrap();
        planning.delete_rel(SPACE, &second.base.id).await.unwrap();
        planning
            .commit(
                "resolution_test",
                "conjunction",
                &serde_json::json!({"claims": [first.base.id, second.base.id]}),
                &(),
            )
            .await
            .unwrap();
        let repo = RelRepository::new(client.clone());
        let mut later = repo.get(SPACE, &first.base.id).await.unwrap().unwrap();
        assert_eq!(later.base.provenance.sources.len(), 2);
        assert_eq!(later.evidence.len(), 2);
        later.confidence = 0.51;
        repo.update(SPACE, &first.base.id, later).await.unwrap();
        let inverse =
            elephant_storage::source_decision::SourceDecisionRepository::new(client.clone())
                .invalidation(SPACE, &source_b.base.id)
                .await
                .unwrap();
        let restored = inverse
            .effects
            .iter()
            .find_map(|effect| match &effect.after {
                Some(CanonicalRecord::Rel(row)) if row.base.id == first.base.id => Some(row),
                _ => None,
            })
            .expect("surviving baseline restored by conjunction invalidation");
        assert_eq!(restored.evidence, first.evidence);
        assert_eq!(
            restored.base.provenance.sources,
            first.base.provenance.sources
        );
        assert_eq!(restored.status, first.status);
        assert_eq!(
            restored.confidence, 0.51,
            "later independent value must survive"
        );
        assert!(!inverse.receipt_keys.is_empty());
        // This oracle prepares the owner inverse. Source lifecycle owns applying
        // it with physical deletion; no claim here that erasure already occurred.
    }
}

#[tokio::test]
async fn learned_predicate_rationale_cannot_cross_source_partition() {
    let client = setup(true).await;
    let mut authority = base(TableName::ConflictGroup);
    authority.provenance.sources = vec![RecordId::new_v4(TableName::Source)];
    let mut registry_base = base(TableName::PredicateRegistry);
    registry_base.provenance.sources = vec![RecordId::new_v4(TableName::Source)];
    let existing = elephant_storage::PredicateRegistryRepository::new(client.clone())
        .create(PredicateRegistry {
            base: registry_base,
            predicate_id: "works_at".into(),
            description: Some("private old rationale".into()),
            status: "active".into(),
            semantics: Some(PredicateSemantics::default()),
            semantics_status: SemanticsStatus::LlmProposed,
            first_seen_at: None,
            usage_count: 0,
            last_used_at: None,
        })
        .await
        .unwrap();
    let planning = PlanningTruthMaintenanceStorage::new(client.clone(), SPACE, None)
        .await
        .unwrap();
    assert!(planning
        .upsert_predicate_semantics(
            SPACE,
            "works_at",
            PredicateSemantics::default(),
            SemanticsStatus::LlmProposed,
            "another source rationale",
            &authority
        )
        .await
        .unwrap()
        .is_none());
    assert!(planning.prepared_plan().unwrap().effects.is_empty());
    assert_eq!(
        elephant_storage::PredicateRegistryRepository::new(client)
            .get(SPACE, &existing.base.id)
            .await
            .unwrap(),
        Some(existing)
    );
}

#[test]
fn maintenance_notifications_do_not_recursively_trigger_claim_decisions() {
    let mut event = OutboxPayload::new(
        SPACE.into(),
        "rel.maintenance".into(),
        "truth_maintenance".into(),
    );
    event.add_record(RecordRef {
        id: RecordId::new_v4(TableName::Rel),
        table: "rel".into(),
        op: EventOperation::Update,
    });
    assert!(crate::truth_maintenance::trigger_events_from_outbox_payload(&event).is_empty());
    event.event_type = "assertion.maintenance".into();
    event.records = vec![RecordRef {
        id: RecordId::new_v4(TableName::Assertion),
        table: "assertion".into(),
        op: EventOperation::Update,
    }];
    assert_eq!(
        crate::truth_maintenance::trigger_events_from_outbox_payload(&event).len(),
        1
    );
}

#[tokio::test]
async fn resolution_decision_epoch_cannot_be_refreshed_after_configuration_change() {
    let client = setup(true).await;
    let (assertion, entity) = manual_assertion(&client).await;
    let mut decision = patch(&assertion, &entity);
    decision.prepared_epoch = Some(
        CanonicalRepository::new(client.clone())
            .epoch(SPACE)
            .await
            .unwrap(),
    );
    let repo = SpaceConfigRepository::new(client.clone());
    let mut config = repo.get_by_space_id(SPACE).await.unwrap().unwrap();
    config.purpose = Some("changed after decision".into());
    repo.update(SPACE, &config.base.id, config.clone())
        .await
        .unwrap();
    assert!(crate::resolution::apply_resolution_patches_with_attempt(
        &client,
        SPACE,
        "old-decision",
        &[decision],
        &crate::resolution::BackoffConfig::default(),
        None,
        None
    )
    .await
    .is_err());
    assert_eq!(
        elephant_storage::AssertionRepository::new(client)
            .get(SPACE, &assertion.base.id)
            .await
            .unwrap(),
        Some(assertion)
    );
}

#[tokio::test]
async fn resolution_artifact_batch_failure_preserves_every_artifact_and_claim() {
    let client = setup(true).await;
    let (assertion, entity) = manual_assertion(&client).await;
    let run = elephant_storage::WorkRunRepository::new(client.clone())
        .create(WorkRun {
            base: base(TableName::WorkRun),
            work_item: Link::new(RecordId::new_v4(TableName::WorkItem)),
            attempt: 1,
            started_at: Utc::now(),
            completed_at: None,
            success: false,
            error: None,
            metrics: WorkRunMetrics::default(),
            worker_id: "test".into(),
            worker_version: "1".into(),
            model_version: None,
            prompt_id: None,
            config_hash: None,
        })
        .await
        .unwrap();
    let first = patch(&assertion, &entity);
    let mut second = first.clone();
    second.assertion_id = RecordId::new_v4(TableName::Assertion);
    let repo = elephant_storage::WorkArtifactRepository::new(client.clone());
    let mut artifacts = Vec::new();
    for patch in [first, second] {
        artifacts.push(
            repo.create(WorkArtifact {
                base: base(TableName::WorkArtifact),
                work_run: Link::new(run.base.id.clone()),
                artifact_type: crate::resolution::RESOLUTION_PATCH_ARTIFACT_TYPE.into(),
                data: serde_json::to_value(patch).unwrap(),
                status: ArtifactStatus::Validated,
                validation_errors: None,
                validated_at: Some(Utc::now()),
                committed_at: None,
            })
            .await
            .unwrap(),
        );
    }
    assert!(
        crate::resolution::ResolutionPatchWorkflow::new(client.clone())
            .commit_resolution_patch_batch(
                SPACE,
                &artifacts
                    .iter()
                    .map(|row| row.base.id.clone())
                    .collect::<Vec<_>>(),
                &crate::resolution::BackoffConfig::default(),
                None
            )
            .await
            .is_err()
    );
    for artifact in artifacts {
        assert_eq!(
            repo.get(SPACE, &artifact.base.id).await.unwrap(),
            Some(artifact)
        );
    }
    assert_eq!(
        elephant_storage::AssertionRepository::new(client.clone())
            .get(SPACE, &assertion.base.id)
            .await
            .unwrap(),
        Some(assertion)
    );
    assert!(elephant_storage::AuditLogRepository::new(client.clone())
        .list(SPACE, 20, None)
        .await
        .unwrap()
        .is_empty());
    assert!(elephant_storage::SystemEventRepository::new(client)
        .list(SPACE, 20, None)
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn derived_conflict_support_shrinks_when_a_claim_is_removed() {
    let client = setup(true).await;
    let initial = claim(&client).await;
    let (first, _) = legacy_claim(&client, initial, "first retained claim").await;
    let mut other = first.clone();
    other.base.id = RecordId::new_v4(TableName::Rel);
    let (second, _) = legacy_claim(&client, other, "second retained claim").await;
    let planning = PlanningTruthMaintenanceStorage::new(client.clone(), SPACE, None)
        .await
        .unwrap();
    let group = planning
        .create_conflict_group(ConflictGroup {
            base: base(TableName::ConflictGroup),
            slot_key: "explicit-selected-claims".into(),
            slot_type: "rel".into(),
            claim_ids: vec![first.base.id.to_string(), second.base.id.to_string()],
            resolved: false,
            resolution_rationale: None,
            detected_at: Utc::now(),
            resolved_at: None,
            adjudicated_claim_set_hash: None,
            adjudicated_at: None,
        })
        .await
        .unwrap();
    planning
        .commit(
            "resolution_test",
            "initial-conflict",
            &serde_json::json!({}),
            &(),
        )
        .await
        .unwrap();
    assert_eq!(group.base.provenance.sources.len(), 2);
    RelRepository::new(client.clone())
        .delete(SPACE, &second.base.id)
        .await
        .unwrap();
    let planning = PlanningTruthMaintenanceStorage::new(client.clone(), SPACE, None)
        .await
        .unwrap();
    let mut updated = group.clone();
    updated.claim_ids = vec![first.base.id.to_string()];
    let updated = planning
        .upsert_unresolved_conflict_group(updated)
        .await
        .unwrap();
    assert_eq!(
        updated.base.provenance.sources,
        first.base.provenance.sources
    );
    planning
        .commit(
            "resolution_test",
            "reduced-conflict",
            &serde_json::json!({}),
            &(),
        )
        .await
        .unwrap();
    let stored = elephant_storage::ConflictGroupRepository::new(client)
        .get(SPACE, &group.base.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.claim_ids, vec![first.base.id.to_string()]);
    assert_eq!(
        stored.base.provenance.sources,
        first.base.provenance.sources
    );
}

#[tokio::test]
async fn active_detection_commits_deterministic_slots_without_a_stale_successor() {
    use crate::truth_maintenance::{execute_tm_detect_conflicts, TriggerEvent};
    for embedded in [true, false] {
        let client = setup(embedded).await;
        let row = claim(&client).await;
        let reader = RepositoryTruthMaintenanceStorage::new(client.clone());
        let result =
            execute_tm_detect_conflicts(&[TriggerEvent::Rel(row.base.id.clone())], SPACE, &reader)
                .await
                .unwrap();
        assert_eq!(result.events_processed, 1);
        assert!(
            result.direct_commits.is_empty(),
            "deterministic work is in the detection transaction"
        );
        let slots = elephant_storage::TruthSlotRepository::new(client.clone())
            .list(SPACE, 100, None)
            .await
            .unwrap();
        assert!(
            !slots.is_empty(),
            "detection must apply its direct slot result"
        );
        // Re-delivery remains a valid operation; it never manufactures a child
        // that must re-prepare solely because its own detection changed epoch.
        let repeated =
            execute_tm_detect_conflicts(&[TriggerEvent::Rel(row.base.id)], SPACE, &reader)
                .await
                .unwrap();
        assert!(repeated.direct_commits.is_empty());
    }
}

#[tokio::test]
async fn semantic_memo_acknowledges_staleness_without_reapplying_a_verdict() {
    use crate::truth_maintenance::{
        execute_tm_commit_with_storage, execute_tm_detect_conflicts,
        resolve_conflict_group_with_storage, TmContext, TriggerEvent,
    };
    let client = setup(true).await;
    let mut first = claim(&client).await;
    let mut second = first.clone();
    second.base.id = RecordId::new_v4(TableName::Rel);
    let alternative = EntityRepository::new(client.clone())
        .create(Entity::new(
            base(TableName::Entity),
            EntityType::new("organization"),
            "Another Lab".into(),
        ))
        .await
        .unwrap();
    second.object = Link::new(alternative.base.id);
    // Promotion requires actual evidence even for independently authored facts.
    // Build a real document and two coherent spans; an empty-evidence candidate
    // intentionally becomes NeedsReview and cannot seed a successful memo.
    let text = "Ada works at Research. Ada works at Another Lab.";
    let document = elephant_storage::DocRepository::new(client.clone())
        .create(Doc {
            base: base(TableName::Doc),
            title: "authored claims".into(),
            text: text.into(),
            sidecar_text: None,
            source: None,
            media_attachments: vec![],
            rendition_key: uuid::Uuid::new_v4().to_string(),
            metadata: serde_json::json!({}),
        })
        .await
        .unwrap();
    let revision = elephant_storage::DocRevisionRepository::new(client.clone())
        .create(DocRevision {
            base: base(TableName::DocRevision),
            doc: Link::new(document.base.id),
            text: text.into(),
            text_hash: crate::extraction::AssertionValidator::hash_quote(text),
            renderer: elephant_types::records::doc_revision::RendererMetadata {
                name: "authored".into(),
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
    for (row, object, start, end) in [
        (&mut first, "Research", 0_u32, 22_u32),
        (&mut second, "Another Lab", 23, 48),
    ] {
        let quote = text.get(start as usize..end as usize).unwrap();
        let (mut assertion, _) = manual_assertion(&client).await;
        assertion.subject_text = "Ada".into();
        assertion.object_text = Some(object.into());
        assertion.predicate_raw = "works_at".into();
        assertion.predicate_id = Some("works_at".into());
        assertion.subject_entity = Some(row.subject.clone());
        assertion.object_entity = Some(row.object.clone());
        assertion.evidence = EvidenceSpan {
            revision: Link::new(revision.base.id.clone()),
            node: None,
            start,
            end,
            quote_hash: crate::extraction::AssertionValidator::hash_quote(quote),
        };
        let assertion = elephant_storage::AssertionRepository::new(client.clone())
            .update(SPACE, &assertion.base.id.clone(), assertion)
            .await
            .unwrap();
        row.evidence = vec![EvidenceRef {
            assertion: Link::new(assertion.base.id),
            span: assertion.evidence,
            time_basis: TimeBasis::ObservedAt,
            time_value: assertion.observed_at,
        }];
    }
    let first = RelRepository::new(client.clone())
        .update(SPACE, &first.base.id.clone(), first)
        .await
        .unwrap();
    let second = RelRepository::new(client.clone())
        .create(second)
        .await
        .unwrap();
    let storage = RepositoryTruthMaintenanceStorage::new(client.clone());
    let detected = execute_tm_detect_conflicts(
        &[
            TriggerEvent::Rel(first.base.id.clone()),
            TriggerEvent::Rel(second.base.id.clone()),
        ],
        SPACE,
        &storage,
    )
    .await
    .unwrap();
    let mut selected = None;
    for id in detected.conflict_group_ids {
        let group = storage
            .get_conflict_group(SPACE, &id)
            .await
            .unwrap()
            .unwrap();
        if group.slot_type == "rel_set" {
            selected = Some(group);
            break;
        }
    }
    let group = selected.expect("real conflicting relationship set");
    let (resolution, input) =
        resolve_conflict_group_with_storage(&TmContext::none(), &group, SPACE, &storage)
            .await
            .unwrap();
    let original_first = storage.get_rel(SPACE, &first.base.id).await.unwrap();
    let original_second = storage.get_rel(SPACE, &second.base.id).await.unwrap();
    let events = elephant_storage::SystemEventRepository::new(client.clone());
    assert!(events
        .list(SPACE, 100, None)
        .await
        .unwrap()
        .iter()
        .all(|row| row.event_type != "predicate_conflict_no_semantics"));
    let fault = PlanningTruthMaintenanceStorage::new(client.clone(), SPACE, None)
        .await
        .unwrap();
    let prepared = execute_tm_commit_with_storage(&resolution, &input, SPACE, &fault)
        .await
        .unwrap();
    let mut request = fault
        .prepare_request(
            "memo_rollback",
            "governance",
            &serde_json::json!({}),
            &prepared,
        )
        .await
        .unwrap();
    assert_eq!(
        request
            .plan
            .outbox
            .iter()
            .filter(|row| row.event_type == "predicate_conflict_no_semantics")
            .count(),
        1
    );
    request.plan.effects.push(CanonicalEffect {
        before: None,
        after: Some(CanonicalRecord::Rel(Box::new(
            original_first.clone().unwrap(),
        ))),
    });
    let receipt_id = request.commit_id.clone();
    let canonical = CanonicalRepository::new(client.clone());
    assert!(canonical.commit(request).await.is_err());
    assert!(canonical
        .receipt(SPACE, &receipt_id)
        .await
        .unwrap()
        .is_none());
    assert_eq!(
        storage.get_rel(SPACE, &first.base.id).await.unwrap(),
        original_first
    );
    assert_eq!(
        storage.get_rel(SPACE, &second.base.id).await.unwrap(),
        original_second
    );
    assert!(events
        .list(SPACE, 100, None)
        .await
        .unwrap()
        .iter()
        .all(|row| row.event_type != "predicate_conflict_no_semantics"));
    execute_tm_commit_with_storage(&resolution, &input, SPACE, &storage)
        .await
        .unwrap();
    let group = storage
        .get_conflict_group(SPACE, &group.base.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        events
            .list(SPACE, 100, None)
            .await
            .unwrap()
            .iter()
            .filter(|row| row.event_type == "predicate_conflict_no_semantics")
            .count(),
        1
    );
    assert!(
        group.resolved,
        "the first verdict must pass real promotion thresholds"
    );
    assert!(
        group.adjudicated_claim_set_hash.is_some(),
        "a durable verdict must carry its memo"
    );
    let first_before = storage
        .get_rel(SPACE, &first.base.id)
        .await
        .unwrap()
        .unwrap();
    let second_before = storage
        .get_rel(SPACE, &second.base.id)
        .await
        .unwrap()
        .unwrap();
    let mut slot = storage
        .find_slot_by_key(SPACE, &group.slot_key)
        .await
        .unwrap()
        .unwrap();
    slot.stale = true;
    slot.stale_reason = Some("explicit re-maintenance".into());
    let id = slot.base.id.clone();
    elephant_storage::TruthSlotRepository::new(client)
        .update(SPACE, &id, slot)
        .await
        .unwrap();
    let (_, memo) =
        resolve_conflict_group_with_storage(&TmContext::none(), &group, SPACE, &storage)
            .await
            .unwrap();
    assert!(memo.skip_commit, "current semantic verdict is reused");
    assert!(
        !storage
            .find_slot_by_key(SPACE, &group.slot_key)
            .await
            .unwrap()
            .unwrap()
            .stale
    );
    assert_eq!(
        storage.get_rel(SPACE, &first.base.id).await.unwrap(),
        Some(first_before)
    );
    assert_eq!(
        storage.get_rel(SPACE, &second.base.id).await.unwrap(),
        Some(second_before)
    );
}

#[tokio::test]
async fn cleared_provenance_never_turns_ledger_backed_policy_into_global_authority() {
    for embedded in [true, false] {
        let client = setup(embedded).await;
        let initial = claim(&client).await;
        let (claim, _) = legacy_claim(&client, initial, "Ada works at Research").await;
        let planning = PlanningTruthMaintenanceStorage::new(client.clone(), SPACE, None)
            .await
            .unwrap();
        planning.observe(&claim).unwrap();
        let mut envelope = claim.base.clone();
        envelope.id = RecordId::new_v4(TableName::PredicateRegistry);
        let registry = PredicateRegistry {
            base: envelope,
            predicate_id: "configured-private".into(),
            description: None,
            status: "active".into(),
            semantics: Some(PredicateSemantics::default()),
            semantics_status: SemanticsStatus::Configured,
            first_seen_at: None,
            usage_count: 0,
            last_used_at: None,
        };
        planning
            .stage_inherited(None, registry.clone(), std::slice::from_ref(&claim.base))
            .unwrap();
        planning
            .commit("policy_scope_test", "ledger", &serde_json::json!({}), &())
            .await
            .unwrap();
        let check = PlanningTruthMaintenanceStorage::new(client.clone(), SPACE, None)
            .await
            .unwrap();
        assert!(check
            .has_source_support(SPACE, &registry.base.id)
            .await
            .unwrap());
        // Deliberately corrupt only the domain row. The sole source ledger
        // remains authoritative and must prevent this apparent public policy.
        let mut changed = registry;
        changed.base.security = base(TableName::PredicateRegistry).security;
        changed.base.subjects.clear();
        changed.base.subjects_state = SubjectsState::Unknown;
        changed.base.provenance.sources.clear();
        let repo = elephant_storage::PredicateRegistryRepository::new(client.clone());
        let changed = repo
            .update(SPACE, &changed.base.id.clone(), changed)
            .await
            .unwrap();
        let check = PlanningTruthMaintenanceStorage::new(client, SPACE, None)
            .await
            .unwrap();
        assert!(crate::truth_maintenance::scoped_predicate(
            Some(changed),
            Some(&base(TableName::Rel)),
            &check
        )
        .await
        .unwrap()
        .is_none());
    }
}

#[tokio::test]
async fn canonical_boundary_futures_leave_room_for_dispatcher_and_runtime_stack() {
    let client = setup(true).await;
    let planning = PlanningTruthMaintenanceStorage::new(client, SPACE, None)
        .await
        .unwrap();
    let request = serde_json::json!({});
    let commit = planning.commit("future_size", "not-polled", &request, &());
    let prepare = planning.prepare_request("future_size", "not-polled", &request, &());
    // The normal test/runtime stack is finite and also holds its callers and
    // nested poll frames. A composed canonical transaction must not occupy it.
    assert!(std::mem::size_of_val(&commit) < 64 * 1024);
    assert!(std::mem::size_of_val(&prepare) < 64 * 1024);
    // No transaction/provider is polled; real rollback tests remain the
    // semantic oracle for this allocation-only boundary change.
}

#[tokio::test]
async fn retained_tm_phase_futures_leave_room_for_original_dispatcher() {
    use crate::truth_maintenance::{
        execute_tm_commit_with_storage, execute_tm_detect_conflicts,
        execute_tm_detect_conflicts_with_storage, execute_tm_recompute,
        resolve_conflict_group_with_storage, CommitInput, RecomputePriority, ResolutionResult,
        TmContext,
    };
    fn future_size<F, Fut>(_: F) -> usize
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future,
    {
        std::mem::size_of::<Fut>()
    }
    let client = setup(true).await;
    let repository = RepositoryTruthMaintenanceStorage::new(client.clone());
    let planning = PlanningTruthMaintenanceStorage::new(client, SPACE, None)
        .await
        .unwrap();
    let context = TmContext::none();
    let resolution = ResolutionResult::NeedsReview {
        reason: "not polled".into(),
    };
    let input = CommitInput::default();
    let conflict =
        crate::truth_maintenance::create_conflict_group("not-polled", "rel", Vec::new(), SPACE);
    // Measure concrete actual repository and overlay generic instantiations
    // without constructing or polling their storage/provider futures.
    let phases = [
        (
            "repository detection",
            future_size(|| execute_tm_detect_conflicts(&[], SPACE, &repository)),
        ),
        (
            "overlay detection",
            future_size(|| execute_tm_detect_conflicts(&[], SPACE, &planning)),
        ),
        (
            "record detection",
            future_size(|| execute_tm_detect_conflicts_with_storage(&[], &[], SPACE, &repository)),
        ),
        (
            "repository application",
            future_size(|| execute_tm_commit_with_storage(&resolution, &input, SPACE, &repository)),
        ),
        (
            "overlay application",
            future_size(|| execute_tm_commit_with_storage(&resolution, &input, SPACE, &planning)),
        ),
        (
            "recompute",
            future_size(|| {
                execute_tm_recompute(&context, SPACE, RecomputePriority::Normal, 100, &repository)
            }),
        ),
        (
            "resolution",
            future_size(|| {
                resolve_conflict_group_with_storage(&context, &conflict, SPACE, &repository)
            }),
        ),
    ];
    for (phase, bytes) in phases {
        assert!(
            bytes <= 128 * 1024,
            "{phase} future occupies {bytes} bytes before runtime frames"
        );
    }
}

async fn resolution_bundle_fixture(
    client: &StorageClient,
    decision: crate::resolution::ResolutionPatch,
) -> (WorkArtifact, WorkArtifact) {
    use crate::scheduler::staged_commit::{
        CommitBundleArtifactData, CommitBundleKind, CommitBundlePayload,
        COMMIT_BUNDLE_ARTIFACT_TYPE,
    };
    let run = elephant_storage::WorkRunRepository::new(client.clone())
        .create(WorkRun {
            base: base(TableName::WorkRun),
            work_item: Link::new(RecordId::new_v4(TableName::WorkItem)),
            attempt: 1,
            started_at: Utc::now(),
            completed_at: Some(Utc::now()),
            success: true,
            error: None,
            metrics: WorkRunMetrics::default(),
            worker_id: "resolution-oracle".into(),
            worker_version: "1".into(),
            model_version: None,
            prompt_id: None,
            config_hash: None,
        })
        .await
        .unwrap();
    let repo = elephant_storage::WorkArtifactRepository::new(client.clone());
    let source = repo
        .create(WorkArtifact {
            base: base(TableName::WorkArtifact),
            work_run: Link::new(run.base.id),
            artifact_type: crate::resolution::RESOLUTION_PATCH_ARTIFACT_TYPE.into(),
            data: serde_json::to_value(&decision).unwrap(),
            status: ArtifactStatus::Validated,
            validation_errors: None,
            validated_at: Some(Utc::now()),
            committed_at: None,
        })
        .await
        .unwrap();
    let mut bundle = source.clone();
    bundle.base.id = RecordId::new_v4(TableName::WorkArtifact);
    bundle.artifact_type = COMMIT_BUNDLE_ARTIFACT_TYPE.into();
    bundle.data = serde_json::to_value(CommitBundleArtifactData {
        source_artifact_id: source.base.id.to_string(),
        source_artifact_type: source.artifact_type.clone(),
        bundle_kind: CommitBundleKind::ResolutionPatchSet,
        payload: CommitBundlePayload::ResolutionPatchSet(decision),
    })
    .unwrap();
    let bundle = repo.create(bundle).await.unwrap();
    (source, bundle)
}

#[tokio::test]
async fn resolution_bundle_commits_source_and_bundle_once_and_rejects_changed_payload() {
    use crate::resolution::{BackoffConfig, ResolutionPatchWorkflow};
    for embedded in [true, false] {
        let client = setup(embedded).await;
        let (assertion, entity) = manual_assertion(&client).await;
        let mut decision = patch(&assertion, &entity);
        decision.prepared_epoch = Some(
            CanonicalRepository::new(client.clone())
                .epoch(SPACE)
                .await
                .unwrap(),
        );
        let (source, bundle) = resolution_bundle_fixture(&client, decision).await;
        let workflow = ResolutionPatchWorkflow::new(client.clone());
        let ids = [bundle.base.id.clone()];
        let committed = workflow
            .commit_resolution_bundle_batch(SPACE, &ids, &BackoffConfig::default(), None)
            .await
            .unwrap();
        assert_eq!(committed.len(), 1);
        assert_eq!(committed[0].status, ArtifactStatus::Committed);
        let repo = elephant_storage::WorkArtifactRepository::new(client.clone());
        let source_after = repo.get(SPACE, &source.base.id).await.unwrap().unwrap();
        assert_eq!(source_after.status, ArtifactStatus::Committed);
        let assertions = elephant_storage::AssertionRepository::new(client.clone());
        let resolved = assertions
            .get(SPACE, &assertion.base.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(resolved.resolution_status, ResolutionStatus::Resolved);
        assert_eq!(resolved.subject_entity, Some(Link::new(entity.base.id)));
        let outbox = elephant_storage::SystemEventRepository::new(client.clone());
        let events = outbox.list(SPACE, 100, None).await.unwrap();
        assert!(!events.is_empty());
        assert_eq!(
            workflow
                .commit_resolution_bundle_batch(SPACE, &ids, &BackoffConfig::default(), None)
                .await
                .unwrap(),
            committed
        );
        assert_eq!(outbox.list(SPACE, 100, None).await.unwrap(), events);

        // Tamper both staged representations consistently: a committed-looking
        // artifact still cannot borrow the original successful request receipt.
        let mut changed_source = source_after;
        changed_source.data["confidence"] = serde_json::json!(0.3);
        repo.update(SPACE, &changed_source.base.id, changed_source.clone())
            .await
            .unwrap();
        let mut changed_bundle = committed[0].clone();
        changed_bundle.data["payload"]["data"]["confidence"] = serde_json::json!(0.3);
        repo.update(SPACE, &changed_bundle.base.id, changed_bundle.clone())
            .await
            .unwrap();
        assert!(workflow
            .commit_resolution_bundle_batch(SPACE, &ids, &BackoffConfig::default(), None)
            .await
            .is_err());
        assert_eq!(
            assertions.get(SPACE, &assertion.base.id).await.unwrap(),
            Some(resolved)
        );
        assert_eq!(outbox.list(SPACE, 100, None).await.unwrap(), events);
    }
}

#[tokio::test]
async fn resolution_bundle_refusals_preserve_all_source_and_bundle_statuses() {
    use crate::resolution::{BackoffConfig, ResolutionPatchWorkflow};
    for embedded in [true, false] {
        for refusal in [
            "later_missing_assertion",
            "stale_epoch",
            "foreign_run",
            "missing_epoch",
        ] {
            let client = setup(embedded).await;
            let (assertion, entity) = manual_assertion(&client).await;
            let mut decision = patch(&assertion, &entity);
            decision.prepared_epoch = Some(
                CanonicalRepository::new(client.clone())
                    .epoch(SPACE)
                    .await
                    .unwrap(),
            );
            let (source, bundle) = resolution_bundle_fixture(&client, decision.clone()).await;
            let mut missing = decision;
            missing.assertion_id = RecordId::new_v4(TableName::Assertion);
            let (other_source, other_bundle) = resolution_bundle_fixture(&client, missing).await;
            let repo = elephant_storage::WorkArtifactRepository::new(client.clone());
            let mut ids = vec![bundle.base.id.clone()];
            match refusal {
                "later_missing_assertion" => ids.push(other_bundle.base.id.clone()),
                "stale_epoch" => {
                    let configs = SpaceConfigRepository::new(client.clone());
                    let mut config = configs.get_by_space_id(SPACE).await.unwrap().unwrap();
                    config.purpose = Some("changed after decision".into());
                    configs
                        .update(SPACE, &config.base.id, config.clone())
                        .await
                        .unwrap();
                }
                "foreign_run" => {
                    let mut changed = bundle.clone();
                    changed.work_run = other_bundle.work_run.clone();
                    repo.update(SPACE, &changed.base.id, changed.clone())
                        .await
                        .unwrap();
                }
                "missing_epoch" => {
                    let mut changed = bundle.clone();
                    changed.data["payload"]["data"]["prepared_epoch"] = serde_json::Value::Null;
                    repo.update(SPACE, &changed.base.id, changed.clone())
                        .await
                        .unwrap();
                }
                _ => unreachable!(),
            }
            let mut originals = Vec::new();
            for id in [
                &source.base.id,
                &bundle.base.id,
                &other_source.base.id,
                &other_bundle.base.id,
            ] {
                originals.push(repo.get(SPACE, id).await.unwrap().unwrap());
            }
            assert!(
                ResolutionPatchWorkflow::new(client.clone())
                    .commit_resolution_bundle_batch(SPACE, &ids, &BackoffConfig::default(), None)
                    .await
                    .is_err(),
                "{refusal}"
            );
            for original in originals {
                assert_eq!(original.status, ArtifactStatus::Validated);
                assert_eq!(
                    repo.get(SPACE, &original.base.id).await.unwrap(),
                    Some(original)
                );
            }
            assert_eq!(
                elephant_storage::AssertionRepository::new(client.clone())
                    .get(SPACE, &assertion.base.id)
                    .await
                    .unwrap(),
                Some(assertion)
            );
            assert!(elephant_storage::SystemEventRepository::new(client)
                .list(SPACE, 100, None)
                .await
                .unwrap()
                .is_empty());
        }
    }
}

#[tokio::test]
async fn staged_resolution_patch_inherits_actual_assertion_and_run_scope() {
    for embedded in [true, false] {
        let client = setup(embedded).await;
        let (mut assertion, entity) = manual_assertion(&client).await;
        assertion.base.security.level = 2;
        assertion.base.subjects_state = SubjectsState::Complete;
        assertion.base.security.labels = vec!["assertion-private".into()];
        assertion
            .base
            .subjects
            .push(Link::new(entity.base.id.clone()));
        assertion = elephant_storage::AssertionRepository::new(client.clone())
            .update(SPACE, &assertion.base.id, assertion.clone())
            .await
            .unwrap();
        let decision = patch(&assertion, &entity);
        let (source, _) = resolution_bundle_fixture(&client, decision.clone()).await;
        let runs = elephant_storage::WorkRunRepository::new(client.clone());
        let mut run = runs
            .get(SPACE, source.work_run.id())
            .await
            .unwrap()
            .unwrap();
        run.base.security.level = 3;
        run.base.security.labels = vec!["run-private".into()];
        run.base.subjects_state = SubjectsState::Partial;
        run = runs.update(SPACE, &run.base.id, run.clone()).await.unwrap();
        let workflow = crate::resolution::ResolutionPatchWorkflow::new(client.clone());
        let artifact = workflow
            .stage_resolution_patch(&run.base.id, decision.clone(), SPACE)
            .await
            .unwrap();
        assert!(contains_authorization(&artifact.base, &assertion.base).unwrap());
        assert!(contains_authorization(&artifact.base, &run.base).unwrap());
        assert_eq!(artifact.base.security.level, 3);
        assert_eq!(artifact.base.subjects_state, SubjectsState::Partial);
        assert!(artifact
            .base
            .security
            .labels
            .contains(&"assertion-private".into()));
        assert!(artifact
            .base
            .security
            .labels
            .contains(&"run-private".into()));
        assert_eq!(
            artifact.base.provenance.work_run_id,
            Some(run.base.id.clone())
        );
        let artifacts = elephant_storage::WorkArtifactRepository::new(client.clone());
        let before = artifacts.list(SPACE, 100, None).await.unwrap();
        run.base.security.policy_id = "incompatible".into();
        runs.update(SPACE, &run.base.id, run.clone()).await.unwrap();
        assert!(workflow
            .stage_resolution_patch(&run.base.id, decision, SPACE)
            .await
            .is_err());
        assert_eq!(artifacts.list(SPACE, 100, None).await.unwrap(), before);
    }
}

#[tokio::test]
async fn contact_fact_comparison_requires_actual_complete_redirect_snapshots() {
    use elephant_types::records::entity::EntityStatus;
    let client = setup(true).await;
    let before = claim(&client).await;
    let entities = EntityRepository::new(client.clone());
    let mut original = entities
        .get(SPACE, before.subject.id())
        .await
        .unwrap()
        .unwrap();
    let terminal = entities
        .create(Entity::new(
            base(TableName::Entity),
            EntityType::new("person"),
            "Resolved Ada".into(),
        ))
        .await
        .unwrap();
    original.status = EntityStatus::Merged;
    original.redirect_to = Some(Link::new(terminal.base.id.clone()));
    original = entities
        .update(SPACE, &original.base.id, original.clone())
        .await
        .unwrap();
    let mut after = before.clone();
    after.base.id = RecordId::new_v4(TableName::Rel);
    after.subject = Link::new(terminal.base.id.clone());
    after.base.subjects.push(after.subject.clone());
    // This is solely a pure factual comparison, never a publication proof.
    assert!(CanonicalRepository::contact_rel_fact_preserved(
        &before,
        &after,
        &[original.clone(), terminal.clone()]
    )
    .unwrap());
    assert!(
        !CanonicalRepository::contact_rel_fact_preserved(&before, &after, &[original.clone()])
            .unwrap()
    );
    let mut private = original.clone();
    private.base.security.level = after.base.security.level + 1;
    assert!(!CanonicalRepository::contact_rel_fact_preserved(
        &before,
        &after,
        &[private, terminal.clone()]
    )
    .unwrap());
    let mut foreign = terminal.clone();
    foreign.base.space_id = "another-space".into();
    assert!(!CanonicalRepository::contact_rel_fact_preserved(
        &before,
        &after,
        &[original.clone(), foreign]
    )
    .unwrap());
    let mut cycle = original.clone();
    cycle.redirect_to = Some(Link::new(cycle.base.id.clone()));
    assert!(!CanonicalRepository::contact_rel_fact_preserved(
        &before,
        &after,
        &[cycle, terminal.clone()]
    )
    .unwrap());
    let mut deleted = terminal;
    deleted.base.deleted_at = Some(Utc::now());
    assert!(!CanonicalRepository::contact_rel_fact_preserved(
        &before,
        &after,
        &[original, deleted]
    )
    .unwrap());
}

#[tokio::test]
async fn contact_dedupe_cannot_turn_unsealed_metadata_into_source_authority() {
    for embedded in [true, false] {
        let client = setup(embedded).await;
        let mut first = claim(&client).await;
        let rels = RelRepository::new(client.clone());
        let sources = elephant_storage::SourceRepository::new(client.clone());
        let mut other = first.clone();
        other.base.id = RecordId::new_v4(TableName::Rel);
        for row in [&mut first, &mut other] {
            let mut source = Source::new(
                base(TableName::Source),
                SourceType::Manual,
                row.base.id.to_string(),
            );
            source.metadata = serde_json::json!({"contact":{"name":"Ada","company":"Research"}});
            let source = sources.create(source).await.unwrap();
            row.base.provenance.sources = vec![source.base.id];
        }
        first = rels
            .update(SPACE, &first.base.id, first.clone())
            .await
            .unwrap();
        other = rels.create(other).await.unwrap();
        let planning = PlanningTruthMaintenanceStorage::new(client.clone(), SPACE, None)
            .await
            .unwrap();
        let mut after = first.clone();
        after.confidence = 0.95;
        assert!(planning
            .merge_relationships(first.clone(), after, &[other.clone()])
            .await
            .is_err());
        assert!(planning.prepared_plan().unwrap().effects.is_empty());
        assert_eq!(rels.get(SPACE, &first.base.id).await.unwrap(), Some(first));
        assert_eq!(rels.get(SPACE, &other.base.id).await.unwrap(), Some(other));
        assert!(elephant_storage::SystemEventRepository::new(client)
            .list(SPACE, 100, None)
            .await
            .unwrap()
            .is_empty());
    }
}

fn generated_wiki_definition(
    entity: &Entity,
    contributors: &[CanonicalRecord],
) -> KnowledgeViewDefinition {
    let mut base = base(TableName::KnowledgeViewDefinition);
    base.security.clone_from(&entity.base.security);
    base.subjects = contributors
        .iter()
        .filter_map(|row| match row {
            CanonicalRecord::Entity(row) => Some(Link::new(row.base.id.clone())),
            _ => None,
        })
        .collect();
    base.subjects_state = SubjectsState::Complete;
    base.provenance.extractor = Some(Extractor {
        name: Some("elephant.wiki_planner".into()),
        version: Some("10".into()),
        model: Some("test".into()),
    });
    base.provenance.sources = contributors
        .iter()
        .flat_map(|row| row.base().provenance.sources.iter().cloned())
        .collect();
    base.provenance.sources.sort();
    base.provenance.sources.dedup();
    KnowledgeViewDefinition {
        base,
        title: entity.display_name.clone(),
        publication_status: KnowledgeViewPublicationStatus::Published,
        related_articles: vec![],
        navigation: None,
        selector: KnowledgeViewSelector::Topic {
            topic: Link::new(entity.base.id.clone()),
            relation_types: vec![],
            max_hops: 1,
            max_members: 20,
        },
    }
}

#[tokio::test]
async fn wiki_definition_bundle_rolls_back_every_definition_audit_event_and_receipt() {
    for embedded in [true, false] {
        let client = setup(embedded).await;
        let entities = EntityRepository::new(client.clone());
        let topic = entities
            .create(Entity::new(
                base(TableName::Entity),
                EntityType::new("topic"),
                "Topic".into(),
            ))
            .await
            .unwrap();
        let organizer = entities
            .create(Entity::new(
                base(TableName::Entity),
                EntityType::new("topic"),
                "Other navigation input".into(),
            ))
            .await
            .unwrap();
        let inputs = vec![topic.clone().record(), organizer.clone().record()];
        let planning = PlanningTruthMaintenanceStorage::new(client.clone(), SPACE, None)
            .await
            .unwrap();
        let key = planning
            .commit_identity("wiki_definition_plan", "bundle")
            .unwrap();
        let mut ids = Vec::new();
        for entity in [&topic, &organizer] {
            let mut definition = generated_wiki_definition(entity, &inputs);
            elephant_storage::canonical_definition::bind_definition_decision(&mut definition, &key)
                .unwrap();
            ids.push(definition.base.id.clone());
            planning
                .stage_wiki_definition(None, definition, &inputs)
                .unwrap();
        }
        planning
            .audit_pending_effects("test complete wiki plan")
            .await
            .unwrap();
        let mut request = planning
            .prepare_request(
                "wiki_definition_plan",
                "bundle",
                &serde_json::json!({"inventory": 1}),
                &ids,
            )
            .await
            .unwrap();
        // The collision is appended after the complete definition/audit effects.
        request.plan.effects.push(CanonicalEffect {
            before: None,
            after: Some(organizer.record()),
        });
        assert!(CanonicalRepository::new(client.clone())
            .commit(request)
            .await
            .is_err());
        let definitions = elephant_storage::KnowledgeViewDefinitionRepository::new(client.clone());
        for id in ids {
            assert!(definitions.get(SPACE, &id).await.unwrap().is_none());
        }
        assert!(elephant_storage::AuditLogRepository::new(client.clone())
            .list(SPACE, 100, None)
            .await
            .unwrap()
            .is_empty());
        assert!(elephant_storage::SystemEventRepository::new(client.clone())
            .list(SPACE, 100, None)
            .await
            .unwrap()
            .is_empty());
        assert!(CanonicalRepository::new(client)
            .receipt(SPACE, &key)
            .await
            .unwrap()
            .is_none());
    }
}

#[tokio::test]
async fn wiki_definition_captures_disconnected_organizer_and_replays_exact_successors() {
    for embedded in [true, false] {
        let client = setup(embedded).await;
        let entities = EntityRepository::new(client.clone());
        let topic = entities
            .create(Entity::new(
                base(TableName::Entity),
                EntityType::new("topic"),
                "Topic".into(),
            ))
            .await
            .unwrap();
        let organizer = entities
            .create(Entity::new(
                base(TableName::Entity),
                EntityType::new("topic"),
                "Other navigation input".into(),
            ))
            .await
            .unwrap();
        let inputs = vec![topic.clone().record(), organizer.clone().record()];
        let preparation = PlanningTruthMaintenanceStorage::new(client.clone(), SPACE, None)
            .await
            .unwrap();
        let mut definition = generated_wiki_definition(&topic, &inputs);
        let key = preparation
            .commit_identity("wiki_definition_plan", "original")
            .unwrap();
        elephant_storage::canonical_definition::bind_definition_decision(&mut definition, &key)
            .unwrap();
        preparation
            .stage_wiki_definition(None, definition.clone(), &inputs)
            .unwrap();
        let mut changed = organizer.clone();
        changed.summary = Some("Changed after the planning prompt".into());
        entities
            .update(SPACE, &organizer.base.id, changed)
            .await
            .unwrap();
        assert!(preparation
            .commit(
                "wiki_definition_plan",
                "original",
                &serde_json::json!({"v": 1}),
                &vec![definition.base.id.clone()]
            )
            .await
            .is_err());
        assert!(
            elephant_storage::KnowledgeViewDefinitionRepository::new(client.clone())
                .get(SPACE, &definition.base.id)
                .await
                .unwrap()
                .is_none()
        );

        let organizer = entities
            .get(SPACE, &organizer.base.id)
            .await
            .unwrap()
            .unwrap();
        let inputs = vec![topic.clone().record(), organizer.record()];
        let current = PlanningTruthMaintenanceStorage::new(client.clone(), SPACE, None)
            .await
            .unwrap();
        let key = current
            .commit_identity("wiki_definition_plan", "current")
            .unwrap();
        elephant_storage::canonical_definition::bind_definition_decision(&mut definition, &key)
            .unwrap();
        current
            .stage_wiki_definition(None, definition.clone(), &inputs)
            .unwrap();
        let output = vec![definition.base.id.clone()];
        current
            .audit_pending_effects("test current inventory")
            .await
            .unwrap();
        current
            .commit(
                "wiki_definition_plan",
                "current",
                &serde_json::json!({"v": 1}),
                &output,
            )
            .await
            .unwrap();
        assert_eq!(
            current
                .replay::<Vec<RecordId>>(
                    "wiki_definition_plan",
                    "current",
                    &serde_json::json!({"v": 1})
                )
                .await
                .unwrap(),
            Some(output)
        );
        assert!(current
            .replay::<Vec<RecordId>>(
                "wiki_definition_plan",
                "current",
                &serde_json::json!({"v": 2})
            )
            .await
            .is_err());
        assert!(CanonicalRepository::new(client)
            .definition_decision_inputs(&definition)
            .await
            .unwrap()
            .is_some());
    }
}

#[tokio::test]
async fn wiki_regeneration_cannot_drop_inventory_restrictions_or_revive_deleted_definition() {
    let client = setup(true).await;
    let mut entity = Entity::new(
        base(TableName::Entity),
        EntityType::new("topic"),
        "Confidential".into(),
    );
    entity.base.security.level = 3;
    let entity = EntityRepository::new(client.clone())
        .create(entity)
        .await
        .unwrap();
    let inputs = vec![entity.clone().record()];
    let generated = generated_wiki_definition(&entity, &inputs);
    for variant in 0..5 {
        let preparation = PlanningTruthMaintenanceStorage::new(client.clone(), SPACE, None)
            .await
            .unwrap();
        let mut after = generated.clone();
        let mut before = None;
        match variant {
            0 => after.base.security.level = 0,
            1 => after.base.subjects.clear(),
            2 => after
                .base
                .provenance
                .sources
                .push(RecordId::new_v4(TableName::Source)),
            3 => {
                let mut old = after.clone();
                old.base.deleted_at = Some(Utc::now());
                before = Some(old);
            }
            _ => {
                let mut old = after.clone();
                old.base.id = RecordId::new_v4(TableName::KnowledgeViewDefinition);
                before = Some(old);
            }
        }
        assert!(
            preparation
                .stage_wiki_definition(before, after, &inputs)
                .is_err(),
            "variant {variant}"
        );
        assert!(preparation.prepared_plan().unwrap().effects.is_empty());
    }
}

#[tokio::test]
async fn wiki_inventory_cannot_omit_the_physical_source_rows_own_contribution() {
    let client = setup(true).await;
    let entity = EntityRepository::new(client.clone())
        .create(Entity::new(
            base(TableName::Entity),
            EntityType::new("topic"),
            "Topic".into(),
        ))
        .await
        .unwrap();
    let mut source_base = base(TableName::Source);
    source_base.subjects_state = SubjectsState::Complete;
    let source = elephant_storage::SourceRepository::new(client.clone())
        .create(Source::new(
            source_base,
            SourceType::Manual,
            "original physical input".into(),
        ))
        .await
        .unwrap();
    assert!(source.base.provenance.sources.is_empty());
    let inputs = vec![entity.clone().record(), source.clone().record()];
    let mut definition = generated_wiki_definition(&entity, &inputs);
    let planning = PlanningTruthMaintenanceStorage::new(client, SPACE, None)
        .await
        .unwrap();
    assert!(planning
        .stage_wiki_definition(None, definition.clone(), &inputs)
        .is_err());
    assert!(planning.prepared_plan().unwrap().effects.is_empty());
    definition.base.provenance.sources = vec![source.base.id];
    planning
        .stage_wiki_definition(None, definition, &inputs)
        .unwrap();
}

#[path = "../../../tests/recall_preview/wiki_definition_oracles.rs"]
mod independent_wiki_definition_oracles;
