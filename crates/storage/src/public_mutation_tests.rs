//! Public adapter oracles against the actual canonical transaction.
#![allow(clippy::expect_used, clippy::unwrap_used)]
use super::*;
use crate::canonical_commit::CanonicalFault;
use crate::{
    AuditLogRepository, EntityRepository, Repository, SpaceConfigRepository, SystemEventRepository,
};
use elephant_types::records::{entity::EntityType, SpaceConfig};

async fn client(embedded: bool) -> StorageClient {
    let client = if embedded {
        StorageClient::new_embedded().await
    } else {
        StorageClient::new_memory().await
    }
    .expect("backend");
    if !embedded {
        client
            .query_checked(concat!(
                include_str!("../../migrations/sql/031_source_publication.surql"),
                "\n",
                include_str!("../../migrations/sql/046_source_support_index_fence.surql")
            ))
            .await
            .expect("canonical schema");
        crate::reference_fence::install_native_reference_guards(&client, &[])
            .await
            .expect("current native reference and physical read guards");
    }
    let config: SpaceConfig = serde_json::from_value(serde_json::json!({
        "id":RecordId::new_v4(TableName::SpaceConfig),"space_id":"lifecycle-test",
        "created_at":Utc::now(),"updated_at":Utc::now(),"created_by":"test",
        "security":{"policy_id":"default","level":0,"labels":[],"handling":[]},
        "provenance":{},"subjects":[],"subjects_state":"complete",
        "embedding_provider":"fake","embedding_model":"test","embedding_dims":8
    }))
    .expect("space fixture");
    SpaceConfigRepository::new(client.clone())
        .create(config)
        .await
        .expect("space");
    client
}
fn entity() -> Entity {
    Entity::new(
        BaseFields::new(
            RecordId::new_v4(TableName::Entity),
            "lifecycle-test".into(),
            "test".into(),
        ),
        EntityType::new("person"),
        "Lifecycle example".into(),
    )
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture, repository, and mutation owners alive through the original conformance phase."
)]
async fn public_mutation_fault_never_leaves_effect_audit_or_outbox_on_either_backend() {
    for embedded in [true, false] {
        let client = client(embedded).await;
        for fault in [
            CanonicalFault::AfterFirstEffect,
            CanonicalFault::BeforeOutbox,
            CanonicalFault::BeforeReceipt,
        ] {
            let record = entity();
            let mut mutation = PublicMutation::begin(client.clone(), "lifecycle-test")
                .await
                .expect("begin");
            mutation.fault = Some(fault);
            assert!(mutation.create(record.clone(), "actor").await.is_err());
            assert!(EntityRepository::new(client.clone())
                .get("lifecycle-test", &record.base.id)
                .await
                .expect("read")
                .is_none());
            assert!(SystemEventRepository::new(client.clone())
                .get_after_seq("lifecycle-test", 0, 100)
                .await
                .expect("events")
                .is_empty());
            // Stable audit identity is part of the failed transaction too.
            let input = serde_json::json!({"space":"lifecycle-test","actor":"actor","action":AuditAction::Create,"before":null,"proposed":semantic_record(&record).expect("semantic"),"additional_effects":[]});
            let commit_id = format!(
                "public-mutation:{}",
                fingerprint(&input).expect("fingerprint")
            );
            assert!(AuditLogRepository::new(client.clone())
                .get(
                    "lifecycle-test",
                    &stable_id(TableName::AuditLog, &commit_id, "audit")
                )
                .await
                .expect("audit")
                .is_none());
        }
    }
}

#[tokio::test]
async fn public_create_retry_has_one_audit_and_one_outbox_event() {
    for embedded in [true, false] {
        let client = client(embedded).await;
        let record = entity();
        let first = PublicMutation::begin(client.clone(), "lifecycle-test")
            .await
            .expect("begin")
            .create(record.clone(), "actor")
            .await
            .expect("create");
        let retry = PublicMutation::begin(client.clone(), "lifecycle-test")
            .await
            .expect("begin")
            .create(record, "actor")
            .await
            .expect("retry");
        assert_eq!(first, retry);
        let events = SystemEventRepository::new(client.clone())
            .get_after_seq("lifecycle-test", 0, 100)
            .await
            .expect("events");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event_type, "entity.created");
    }
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture, repository, and mutation owners alive through the original conformance phase."
)]
async fn stale_delete_and_restore_refuse_reclassification_on_both_backends() {
    for embedded in [true, false] {
        let client = client(embedded).await;
        let repo = EntityRepository::new(client.clone());
        let row = repo.create(entity()).await.expect("create");
        let mutation = PublicMutation::begin(client.clone(), "lifecycle-test")
            .await
            .expect("begin");
        let mut reclassified = row.clone();
        reclassified.base.security.level = 3;
        repo.update("lifecycle-test", &row.base.id, reclassified.clone())
            .await
            .expect("reclassify");
        assert!(mutation.delete(&row, "actor").await.is_err());
        let fresh = repo
            .get("lifecycle-test", &row.base.id)
            .await
            .expect("read")
            .expect("row");
        let deleted = PublicMutation::begin(client.clone(), "lifecycle-test")
            .await
            .expect("begin")
            .delete(&fresh, "actor")
            .await
            .expect("delete");
        let stale_restore = PublicMutation::begin(client.clone(), "lifecycle-test")
            .await
            .expect("begin");
        let mut newer = deleted.clone();
        newer.base.security.labels.push("new-classification".into());
        // Trusted maintenance changes the soft-deleted record through an exact canonical effect.
        let authority = CanonicalRepository::new(client.clone());
        authority
            .commit(CanonicalCommit {
                space_id: "lifecycle-test".into(),
                commit_id: format!("reclassify:{embedded}"),
                input_fingerprint: "reclassify".into(),
                replay_output: None,
                publication: None,
                maintenance: None,
                finalization: None,
                plan: CanonicalPlan {
                    extension_effects: Vec::new(),
                    observed_epoch: authority.epoch("lifecycle-test").await.expect("epoch"),
                    dependencies: vec![],
                    effects: vec![CanonicalEffect {
                        before: Some(deleted.clone().canonical()),
                        after: Some(newer.clone().canonical()),
                    }],
                    outbox: vec![],
                },
            })
            .await
            .expect("reclassify deleted");
        assert!(stale_restore.restore(&deleted, "actor").await.is_err());
        let live = repo
            .get_with_deleted("lifecycle-test", &row.base.id)
            .await
            .expect("read")
            .expect("row");
        assert_eq!(live.base.security, newer.base.security);
        assert!(live.base.deleted_at.is_some());
    }
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture, repository, and mutation owners alive through the original conformance phase."
)]
async fn audit_inverse_preserves_current_envelope_and_refuses_later_changes() {
    for embedded in [true, false] {
        let client = client(embedded).await;
        let repo = EntityRepository::new(client.clone());
        let before = repo.create(entity()).await.expect("create");
        let deleted = PublicMutation::begin(client.clone(), "lifecycle-test")
            .await
            .expect("begin")
            .delete(&before, "actor")
            .await
            .expect("delete");
        let input = serde_json::json!({"space":"lifecycle-test","actor":"actor","action":AuditAction::Delete,"before":before,"proposed":semantic_record(&deleted).expect("semantic"),"additional_effects":[]});
        let commit_id = format!(
            "public-mutation:{}",
            fingerprint(&input).expect("fingerprint")
        );
        let audit = AuditLogRepository::new(client.clone())
            .get(
                "lifecycle-test",
                &stable_id(TableName::AuditLog, &commit_id, "audit"),
            )
            .await
            .expect("read audit")
            .expect("audit");
        let inverse = PreparedAuditRevert::prepare(client.clone(), "lifecycle-test", &audit)
            .await
            .expect("prepare");
        inverse.commit("actor").await.expect("restore");
        assert!(repo
            .get("lifecycle-test", &before.base.id)
            .await
            .expect("read")
            .is_some());
        assert!(
            PreparedAuditRevert::prepare(client.clone(), "lifecycle-test", &audit)
                .await
                .is_err()
        );
    }
}

#[test]
fn inherited_public_envelope_never_upgrades_subject_completeness_or_lowers_security() {
    use elephant_types::records::base::SubjectsState;
    let mut output = entity().base;
    output.subjects_state = SubjectsState::Complete;
    let mut input = output.clone();
    input.security.level = 5;
    input.security.labels.push("restricted".into());
    input.security.handling.push("no-export".into());
    input.subjects_state = SubjectsState::Partial;
    input
        .subjects
        .push(elephant_types::Link::new(RecordId::new_v4(
            TableName::Entity,
        )));
    inherit_public_envelope(&mut output, &input).expect("same policy");
    assert_eq!(output.security, input.security);
    assert_eq!(output.subjects_state, SubjectsState::Partial);
    assert_eq!(output.subjects, input.subjects);
    let mut other = input;
    other.security.policy_id = "different-policy".into();
    assert!(inherit_public_envelope(&mut output, &other).is_err());
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture, repository, and mutation owners alive through the original conformance phase."
)]
async fn staged_metadata_updates_share_audit_outbox_and_rollback() {
    for embedded in [true, false] {
        let client = client(embedded).await;
        let repo = EntityRepository::new(client.clone());
        let first = repo.create(entity()).await.expect("first");
        let second = repo.create(entity()).await.expect("second");
        let mut proposed_first = first.clone();
        proposed_first.metadata = serde_json::json!({"angel_protectees":["subject-a"]});
        let mut proposed_second = second.clone();
        proposed_second.metadata = serde_json::json!({"angel_protectees":["subject-a"]});
        let mut mutation = PublicMutation::begin(client.clone(), "lifecycle-test")
            .await
            .expect("begin");
        mutation
            .stage_replace(&second, proposed_second.clone())
            .expect("stage");
        mutation.fault = Some(CanonicalFault::BeforeOutbox);
        assert!(mutation
            .replace(&first, proposed_first.clone(), false, "actor")
            .await
            .is_err());
        assert_eq!(
            repo.get("lifecycle-test", &first.base.id)
                .await
                .expect("first")
                .expect("row"),
            first
        );
        assert_eq!(
            repo.get("lifecycle-test", &second.base.id)
                .await
                .expect("second")
                .expect("row"),
            second
        );
        let mut mutation = PublicMutation::begin(client.clone(), "lifecycle-test")
            .await
            .expect("begin");
        mutation
            .stage_replace(&second, proposed_second)
            .expect("stage");
        mutation
            .replace(&first, proposed_first, false, "actor")
            .await
            .expect("commit");
        let events = SystemEventRepository::new(client.clone())
            .get_after_seq("lifecycle-test", 0, 100)
            .await
            .expect("events");
        assert_eq!(events.len(), 1);
        let records = events[0].payload["records"].as_array().expect("records");
        assert_eq!(records.len(), 2);
        assert!(records.iter().all(|record| record["op"] == "update"));
    }
}

#[test]
fn legacy_audit_snapshot_uses_record_id_but_rejects_conflicting_identity() {
    let row = entity();
    let mut snapshot = serde_json::to_value(&row).expect("snapshot");
    snapshot.as_object_mut().expect("object").remove("id");
    let decoded = decode_snapshot(&row.base.id, snapshot.clone()).expect("legacy snapshot");
    assert_eq!(decoded.base().id, row.base.id);
    snapshot["id"] = serde_json::to_value(RecordId::new_v4(TableName::Entity)).expect("id");
    assert!(decode_snapshot(&row.base.id, snapshot).is_err());
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture, repository, and mutation owners alive through the original conformance phase."
)]
async fn withdrawn_source_refuses_prepared_restore_and_private_create_replay() {
    use crate::SourceRepository;
    use elephant_types::records::source::{Source, SourceType};
    for embedded in [true, false] {
        let client = client(embedded).await;
        let sources = SourceRepository::new(client.clone());
        let source = sources
            .create(Source::new(
                BaseFields::new(
                    RecordId::new_v4(TableName::Source),
                    "lifecycle-test".into(),
                    "test".into(),
                ),
                SourceType::Manual,
                "lifecycle-source".into(),
            ))
            .await
            .expect("legacy source");
        let mut input = entity();
        input.base.provenance.sources.push(source.base.id.clone());
        let created = PublicMutation::begin(client.clone(), "lifecycle-test")
            .await
            .expect("begin")
            .create(input.clone(), "actor")
            .await
            .expect("source-supported creation");
        let deleted = PublicMutation::begin(client.clone(), "lifecycle-test")
            .await
            .expect("begin")
            .delete(&created, "actor")
            .await
            .expect("delete");
        let restore = PublicMutation::begin(client.clone(), "lifecycle-test")
            .await
            .expect("prepare before withdrawal");
        sources
            .delete("lifecycle-test", &source.base.id)
            .await
            .expect("trusted source withdrawal");
        assert!(restore.restore(&deleted, "actor").await.is_err());
        assert!(PublicMutation::begin(client.clone(), "lifecycle-test")
            .await
            .expect("begin replay")
            .create(input, "actor")
            .await
            .is_err());
        let current = EntityRepository::new(client)
            .get_with_deleted("lifecycle-test", &created.base.id)
            .await
            .expect("current row")
            .expect("tombstone");
        assert_eq!(current, deleted);
    }
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture, repository, and mutation owners alive through the original conformance phase."
)]
async fn metadata_correction_across_policies_keeps_separate_atomic_event_envelopes() {
    for embedded in [true, false] {
        let client = client(embedded).await;
        let repo = EntityRepository::new(client.clone());
        let first = repo.create(entity()).await.expect("first policy");
        let mut second = entity();
        second.base.security.policy_id = "other-policy".into();
        second.base.security.level = 4;
        let second = repo.create(second).await.expect("second policy");
        let mut changed_first = first.clone();
        changed_first.metadata = serde_json::json!({"attribution":["subject-a"]});
        let mut changed_second = second.clone();
        changed_second.metadata = changed_first.metadata.clone();
        let mut mutation = PublicMutation::begin(client.clone(), "lifecycle-test")
            .await
            .expect("begin");
        mutation
            .stage_replace(&second, changed_second)
            .expect("stage");
        mutation
            .replace(&first, changed_first, false, "actor")
            .await
            .expect("atomic cross-policy update");
        let events = SystemEventRepository::new(client)
            .get_after_seq("lifecycle-test", 0, 100)
            .await
            .expect("events");
        assert_eq!(events.len(), 2);
        for event in events {
            let records = event.payload["records"].as_array().expect("records");
            assert_eq!(records.len(), 1);
            let expected = if event.base.security.policy_id == "default" {
                &first
            } else {
                &second
            };
            assert_eq!(event.base.security, expected.base.security);
            assert_eq!(
                records[0]["id"],
                serde_json::to_value(&expected.base.id).expect("id")
            );
        }
    }
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture, repository, and mutation owners alive through the original conformance phase."
)]
async fn relationship_evidence_observation_checks_real_bytes_and_fences_document_changes() {
    use crate::{DocRepository, DocRevisionRepository};
    use elephant_types::records::assertion::EvidenceSpan;
    use elephant_types::records::doc_revision::RendererMetadata;
    use elephant_types::records::{DocRevision, RevisionStatus};
    use elephant_types::Link;
    for embedded in [true, false] {
        let client = client(embedded).await;
        let docs = DocRepository::new(client.clone());
        let doc = docs
            .create(Doc {
                base: BaseFields::new(
                    RecordId::new_v4(TableName::Doc),
                    "lifecycle-test".into(),
                    "test".into(),
                ),
                title: "Observed evidence".into(),
                text: "Alice met Bob — café".into(),
                source: None,
                sidecar_text: None,
                media_attachments: vec![],
                rendition_key: "evidence".into(),
                metadata: serde_json::json!({}),
            })
            .await
            .expect("doc");
        let revision = DocRevisionRepository::new(client.clone())
            .create(DocRevision {
                base: BaseFields::new(
                    RecordId::new_v4(TableName::DocRevision),
                    "lifecycle-test".into(),
                    "test".into(),
                ),
                doc: Link::new(doc.base.id.clone()),
                text: doc.text.clone(),
                text_hash: format!("sha256:{:x}", Sha256::digest(doc.text.as_bytes())),
                renderer: RendererMetadata {
                    name: "fixture".into(),
                    version: "1".into(),
                    config_hash: None,
                },
                manifest: None,
                supersedes: None,
                status: RevisionStatus::Active,
                rendered_at: Utc::now(),
            })
            .await
            .expect("revision");
        let span = EvidenceSpan {
            revision: Link::new(revision.base.id.clone()),
            node: None,
            start: 0,
            end: u32::try_from(doc.text.len()).unwrap(),
            quote_hash: format!("sha256:{:x}", Sha256::digest(doc.text.as_bytes())),
        };
        let mut mutation = PublicMutation::begin(client.clone(), "lifecycle-test")
            .await
            .expect("begin");
        let bare_hash = format!("{:x}", Sha256::digest(doc.text.as_bytes()));
        for hash in [bare_hash.clone(), format!("sha256:{bare_hash}")] {
            let mut valid = span.clone();
            valid.quote_hash = hash;
            let (observed_doc, observed_revision, _) = mutation
                .observe_evidence(&valid)
                .await
                .expect("actual producer or renderer evidence encoding");
            assert_eq!(observed_doc, doc);
            assert_eq!(observed_revision, revision);
        }
        for hash in [
            "sha256:incorrect".into(),
            format!("SHA256:{bare_hash}"),
            format!("sha256:sha256:{bare_hash}"),
            format!(" {bare_hash}"),
            format!("{bare_hash}\n"),
            "0".repeat(64),
        ] {
            let mut forged = span.clone();
            forged.quote_hash = hash;
            assert!(mutation.observe_evidence(&forged).await.is_err());
        }
        // An authentic digest cannot repair an empty range, a partial UTF-8
        // codepoint or a different slice of the same immutable revision.
        for (start, end, hash) in [
            (0, 0, format!("{:x}", Sha256::digest(b""))),
            (0, 15, bare_hash.clone()),
            (0, 5, bare_hash.clone()),
            (span.end, span.end + 1, bare_hash),
        ] {
            let mut forged = span.clone();
            forged.start = start;
            forged.end = end;
            forged.quote_hash = hash;
            assert!(mutation.observe_evidence(&forged).await.is_err());
        }
        let mut reclassified = doc.clone();
        reclassified.base.security.level = 4;
        docs.update("lifecycle-test", &doc.base.id, reclassified)
            .await
            .expect("reclassify");
        assert!(mutation.create(entity(), "actor").await.is_err());
    }
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture, repository, and mutation owners alive through the original conformance phase."
)]
#[expect(
    clippy::too_many_lines,
    reason = "Keep the complete fault and recovery sequence together with its original assertions."
)]
async fn cited_relationship_tuple_cannot_be_rewritten_under_unchanged_assertion() {
    use crate::{AssertionRepository, DocRepository, DocRevisionRepository, RelRepository};
    use elephant_types::records::assertion::{AssertionRetryState, EvidenceSpan, ResolutionStatus};
    use elephant_types::records::doc_revision::{RendererMetadata, RevisionStatus};
    use elephant_types::records::rel::{ClaimStatus, EvidenceRef, TimeBasis};
    use elephant_types::records::DocRevision;
    use elephant_types::Link;
    for embedded in [true, false] {
        let client = client(embedded).await;
        let entities = EntityRepository::new(client.clone());
        let subject = entities.create(entity()).await.unwrap();
        let object = entities.create(entity()).await.unwrap();
        let doc = DocRepository::new(client.clone())
            .create(Doc {
                base: BaseFields::new(
                    RecordId::new_v4(TableName::Doc),
                    "lifecycle-test".into(),
                    "test".into(),
                ),
                title: "Original claim".into(),
                text: "Alice opened laboratory".into(),
                source: None,
                sidecar_text: None,
                media_attachments: vec![],
                rendition_key: "original".into(),
                metadata: serde_json::json!({}),
            })
            .await
            .unwrap();
        let revision = DocRevisionRepository::new(client.clone())
            .create(DocRevision {
                base: BaseFields::new(
                    RecordId::new_v4(TableName::DocRevision),
                    "lifecycle-test".into(),
                    "test".into(),
                ),
                doc: Link::new(doc.base.id.clone()),
                text: doc.text.clone(),
                text_hash: format!("sha256:{:x}", Sha256::digest(doc.text.as_bytes())),
                renderer: RendererMetadata {
                    name: "fixture".into(),
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
        let span = EvidenceSpan {
            revision: Link::new(revision.base.id.clone()),
            node: None,
            start: 0,
            end: u32::try_from(doc.text.len()).unwrap(),
            quote_hash: revision.text_hash.clone(),
        };
        let assertion = AssertionRepository::new(client.clone())
            .create(Assertion {
                base: BaseFields::new(
                    RecordId::new_v4(TableName::Assertion),
                    "lifecycle-test".into(),
                    "test".into(),
                ),
                subject_text: "Alice".into(),
                predicate_raw: "opened".into(),
                predicate_id: Some("opened".into()),
                object_text: Some("laboratory".into()),
                object_literal: None,
                evidence: span.clone(),
                source_event_time: None,
                observed_at: Utc::now(),
                asserted_at: Utc::now(),
                assertion_key: "original-claim".into(),
                resolution_status: ResolutionStatus::Resolved,
                resolution_method: Some("fixture".into()),
                resolution_confidence: Some(0.9),
                retry_state: AssertionRetryState::default(),
                subject_entity: Some(Link::new(subject.base.id.clone())),
                object_entity: Some(Link::new(object.base.id.clone())),
                confidence: 0.9,
                subject_entity_type: None,
                object_entity_type: None,
                significance: None,
                lifecycle_tier: "active".into(),
            })
            .await
            .unwrap();
        let rel = Rel {
            base: BaseFields::new(
                RecordId::new_v4(TableName::Rel),
                "lifecycle-test".into(),
                "test".into(),
            ),
            subject: Link::new(subject.base.id.clone()),
            object: Link::new(object.base.id.clone()),
            rel_type: "opened".into(),
            status: ClaimStatus::Approved,
            confidence: 0.9,
            validity_window: None,
            temporal_audit: None,
            evidence: vec![EvidenceRef {
                assertion: Link::new(assertion.base.id.clone()),
                span,
                time_basis: TimeBasis::ObservedAt,
                time_value: assertion.observed_at,
            }],
        };
        let saved = PublicMutation::begin(client.clone(), "lifecycle-test")
            .await
            .unwrap()
            .create(rel, "author")
            .await
            .unwrap();
        let events_before = SystemEventRepository::new(client.clone())
            .get_after_seq("lifecycle-test", 0, 100)
            .await
            .unwrap();
        let mut changed = saved.clone();
        changed.rel_type = "destroyed".into();
        assert!(PublicMutation::begin(client.clone(), "lifecycle-test")
            .await
            .unwrap()
            .replace(&saved, changed, false, "author")
            .await
            .is_err());
        for subject_changed in [true, false] {
            let mut changed = saved.clone();
            if subject_changed {
                changed.subject = Link::new(RecordId::new_v4(TableName::Entity));
            } else {
                changed.object = Link::new(RecordId::new_v4(TableName::Entity));
            }
            assert!(PublicMutation::begin(client.clone(), "lifecycle-test")
                .await
                .unwrap()
                .replace(&saved, changed, false, "author")
                .await
                .is_err());
        }
        assert_eq!(
            RelRepository::new(client.clone())
                .get("lifecycle-test", &saved.base.id)
                .await
                .unwrap()
                .unwrap(),
            saved
        );
        assert_eq!(
            SystemEventRepository::new(client.clone())
                .get_after_seq("lifecycle-test", 0, 100)
                .await
                .unwrap(),
            events_before
        );
        let mut correction = saved.clone();
        correction.confidence = 0.7;
        correction.status = ClaimStatus::Disputed;
        let corrected = PublicMutation::begin(client.clone(), "lifecycle-test")
            .await
            .unwrap()
            .replace(&saved, correction, false, "author")
            .await
            .unwrap();
        assert_eq!(corrected.rel_type, "opened");
        assert_eq!(corrected.evidence, saved.evidence);
        // Entity-to-literal claims use the same typed consistency boundary.
        let mut literal_assertion = assertion.clone();
        literal_assertion.base.id = RecordId::new_v4(TableName::Assertion);
        literal_assertion.predicate_raw = "capacity".into();
        literal_assertion.predicate_id = Some("capacity".into());
        literal_assertion.object_entity = None;
        literal_assertion.object_text = None;
        literal_assertion.object_literal = Some(serde_json::json!(42));
        literal_assertion.assertion_key = "literal-claim".into();
        let literal_assertion = AssertionRepository::new(client.clone())
            .create(literal_assertion)
            .await
            .unwrap();
        let attribute = elephant_types::records::Attribute {
            base: BaseFields::new(
                RecordId::new_v4(TableName::Attribute),
                "lifecycle-test".into(),
                "test".into(),
            ),
            subject: saved.subject.clone(),
            attr_type: "capacity".into(),
            value: elephant_types::records::attribute::Literal::Integer(42),
            status: ClaimStatus::Approved,
            confidence: 0.9,
            validity_window: None,
            temporal_audit: None,
            evidence: vec![EvidenceRef {
                assertion: Link::new(literal_assertion.base.id.clone()),
                span: literal_assertion.evidence.clone(),
                time_basis: TimeBasis::ObservedAt,
                time_value: literal_assertion.observed_at,
            }],
        };
        let mut changed = attribute.clone();
        changed.value = elephant_types::records::attribute::Literal::Integer(99);
        let mut mutation = PublicMutation::begin(client.clone(), "lifecycle-test")
            .await
            .unwrap();
        assert!(mutation
            .observe_attribute_assertion(&changed, &literal_assertion)
            .await
            .is_err());
        mutation
            .stage_create(CanonicalRecord::Attribute(Box::new(changed)))
            .unwrap();
        assert!(mutation
            .replace(&corrected, corrected.clone(), false, "author")
            .await
            .is_err());
        assert!(crate::AttributeRepository::new(client.clone())
            .get("lifecycle-test", &attribute.base.id)
            .await
            .unwrap()
            .is_none());
        let mut mutation = PublicMutation::begin(client.clone(), "lifecycle-test")
            .await
            .unwrap();
        mutation
            .observe_attribute_assertion(&attribute, &literal_assertion)
            .await
            .unwrap();
        mutation
            .stage_create(CanonicalRecord::Attribute(Box::new(attribute.clone())))
            .unwrap();
        mutation
            .replace(&corrected, corrected.clone(), false, "author")
            .await
            .unwrap();
        assert_eq!(
            crate::AttributeRepository::new(client.clone())
                .get("lifecycle-test", &attribute.base.id)
                .await
                .unwrap()
                .unwrap(),
            attribute
        );
        // Independently authored relationships without a cited source remain editable.
        let mut independent = saved.clone();
        independent.base.id = RecordId::new_v4(TableName::Rel);
        independent.rel_type = "independent_claim".into();
        independent.evidence.clear();
        PublicMutation::begin(client.clone(), "lifecycle-test")
            .await
            .unwrap()
            .create(independent, "author")
            .await
            .unwrap();
    }
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture, repository, and mutation owners alive through the original conformance phase."
)]
#[expect(
    clippy::too_many_lines,
    reason = "Keep the complete fault and recovery sequence together with its original assertions."
)]
async fn predicate_approval_fences_artifact_and_commits_alias_audit_outbox_together() {
    use elephant_types::records::predicate::{AliasScope, AliasStatus, AliasType};
    use elephant_types::records::work::ArtifactStatus;
    use elephant_types::Link;
    for embedded in [true, false] {
        let client = client(embedded).await;
        let mut base = BaseFields::new(
            RecordId::new_v4(TableName::WorkArtifact),
            "lifecycle-test".into(),
            "producer".into(),
        );
        base.security.level = 3;
        let observed = WorkArtifact {
            base,
            work_run: Link::new(RecordId::new_v4(TableName::WorkRun)),
            artifact_type: "predicate_proposal".into(),
            data: serde_json::json!({"predicate_id":"works_at"}),
            status: ArtifactStatus::Validated,
            validation_errors: None,
            validated_at: Some(Utc::now()),
            committed_at: None,
        };
        let artifacts = crate::WorkArtifactRepository::new(client.clone());
        artifacts.create(observed.clone()).await.expect("artifact");
        let mut alias_base = observed.base.clone();
        alias_base.id = RecordId::new_v4(TableName::PredicateAlias);
        let alias = PredicateAlias {
            base: alias_base,
            alias: "employed by".into(),
            alias_type: AliasType::RawString,
            source_plugin: Some("proposal".into()),
            confidence: 0.95,
            status: AliasStatus::Active,
            scope: AliasScope::Global,
            predicate_id: "works_at".into(),
            confidence_version: 1,
            usage_count: 0,
        };
        let mut committed = observed.clone();
        committed.status = ArtifactStatus::Committed;
        committed.committed_at = Some(Utc::now());
        let mut mutation = PublicMutation::begin(client.clone(), "lifecycle-test")
            .await
            .expect("begin");
        mutation
            .stage_create(CanonicalRecord::PredicateAlias(Box::new(alias.clone())))
            .expect("stage");
        mutation.fault = Some(CanonicalFault::AfterFirstEffect);
        assert!(mutation
            .replace(&observed, committed.clone(), false, "approver")
            .await
            .is_err());
        assert!(crate::PredicateAliasRepository::new(client.clone())
            .get("lifecycle-test", &alias.base.id)
            .await
            .expect("alias")
            .is_none());
        assert_eq!(
            artifacts
                .get("lifecycle-test", &observed.base.id)
                .await
                .expect("artifact")
                .expect("present")
                .status,
            ArtifactStatus::Validated
        );
        assert!(AuditLogRepository::new(client.clone())
            .list("lifecycle-test", 100, None)
            .await
            .expect("audit")
            .is_empty());
        assert!(SystemEventRepository::new(client.clone())
            .get_after_seq("lifecycle-test", 0, 100)
            .await
            .expect("outbox")
            .is_empty());
        mutation.fault = None;
        mutation
            .replace(&observed, committed.clone(), false, "approver")
            .await
            .expect("atomic approval");
        let aliases = crate::PredicateAliasRepository::new(client.clone());
        assert_eq!(
            aliases
                .get("lifecycle-test", &alias.base.id)
                .await
                .expect("alias")
                .expect("present")
                .base
                .security
                .level,
            3
        );
        assert_eq!(
            AuditLogRepository::new(client.clone())
                .list("lifecycle-test", 100, None)
                .await
                .expect("audit")
                .len(),
            2
        );
        assert_eq!(
            SystemEventRepository::new(client.clone())
                .get_after_seq("lifecycle-test", 0, 100)
                .await
                .expect("outbox")
                .len(),
            1
        );
        let stale = PublicMutation::begin(client, "lifecycle-test")
            .await
            .expect("begin stale");
        assert!(stale
            .replace(&observed, committed, false, "different actor")
            .await
            .is_err());
    }
}
