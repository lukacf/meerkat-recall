//! Whole-transaction regression oracles; authored for both exposed backends.
#![allow(clippy::expect_used, clippy::unwrap_used)]

#[path = "canonical_effect_order_tests.rs"]
mod effect_order;

#[path = "canonical_outbox_sequence_tests.rs"]
mod outbox_sequence;

#[path = "canonical_audit_support_tests.rs"]
mod audit_support;

use super::*;
use crate::{Repository, SpaceConfigRepository, StorageConfig};
use elephant_types::records::{
    entity::EntityType, AuditAction, ClaimStatus, SourceType, SpaceConfig, WorkItemStatus,
};
use elephant_types::TableName;

async fn client(embedded: bool) -> StorageClient {
    let client = if embedded {
        StorageClient::new_embedded().await
    } else {
        StorageClient::new(StorageConfig::default()).await
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
            .expect("schema");
    }
    let config: SpaceConfig = serde_json::from_value(serde_json::json!({
        "id":RecordId::new_v4(TableName::SpaceConfig),"space_id":"named-a",
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

fn entity(name: &str) -> Entity {
    Entity::new(
        BaseFields::new(
            RecordId::new_v4(TableName::Entity),
            "named-a".into(),
            "test".into(),
        ),
        EntityType::new("project"),
        name.into(),
    )
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
#[expect(
    clippy::too_many_lines,
    reason = "Keep the complete ordered conformance scenario and its failure assertions together."
)]
async fn atomic_effects_outbox_receipt(embedded: bool) {
    let client = client(embedded).await;
    let repository = CanonicalRepository::new(client.clone());
    let first = entity("first complete effect");
    let second = entity("second complete effect");
    let mapping = IdMap {
        base: BaseFields::new(
            RecordId::new_v4(TableName::IdMap),
            "named-a".into(),
            "test".into(),
        ),
        legacy_id: "legacy-record".into(),
        record: first.base.id.clone(),
        source_system: "test-import".into(),
    };
    let job = ReprocessJob {
        base: BaseFields::new(
            RecordId::new_v4(TableName::ReprocessJob),
            "named-a".into(),
            "test".into(),
        ),
        name: "accepted intent".into(),
        selection_criteria: elephant_types::records::reprocess::SelectionCriteria::default(),
        target_stages: elephant_types::records::reprocess::default_reprocess_target_stages(),
        status: elephant_types::records::reprocess::ReprocessStatus::Running,
        progress: elephant_types::records::reprocess::ReprocessProgress::default(),
        priority: 0,
        budget_limit_euros: None,
        max_sources_per_day: None,
        started_at: Some(Utc::now()),
        completed_at: None,
        error: None,
        reason: None,
    };
    ensure_extension_epoch_guards(&client, &["test_merge_metadata".into()])
        .await
        .expect("extension fences");
    let extension_id = surrealdb::sql::Thing::from(("test_merge_metadata", "atomic"));
    let extension = extension_row(&extension_id, "derived", None);
    let event = SystemEvent {
        base: BaseFields::new(
            RecordId::new_v4(TableName::SystemEvent),
            "named-a".into(),
            "test".into(),
        ),
        seq: 0,
        event_type: "records.changed".into(),
        payload: serde_json::json!({"records":[{"id":first.base.id,"op":"create"},{"id":second.base.id,"op":"create"}]}),
    };
    let request = CanonicalCommit {
        space_id: "named-a".into(),
        commit_id: "atomic-regression".into(),
        input_fingerprint: "same-input".into(),
        replay_output: Some(serde_json::json!({"pending": ["stable-successor"]})),
        publication: None,
        maintenance: None,
        finalization: None,
        plan: CanonicalPlan {
            extension_effects: vec![crate::canonical_extension::CanonicalExtensionEffect {
                id: extension_id.clone(),
                before: None,
                after: Some(extension.clone()),
            }],
            observed_epoch: repository.epoch("named-a").await.expect("epoch"),
            dependencies: vec![],
            effects: vec![
                CanonicalEffect {
                    before: None,
                    after: Some(CanonicalRecord::IdMap(Box::new(mapping.clone()))),
                },
                CanonicalEffect {
                    before: None,
                    after: Some(CanonicalRecord::ReprocessJob(Box::new(job.clone()))),
                },
                CanonicalEffect {
                    before: None,
                    after: Some(CanonicalRecord::Entity(Box::new(first.clone()))),
                },
                CanonicalEffect {
                    before: None,
                    after: Some(CanonicalRecord::Entity(Box::new(second.clone()))),
                },
            ],
            outbox: vec![event.clone()],
        },
    };
    for fault in [
        CanonicalFault::AfterFirstEffect,
        CanonicalFault::BeforeOutbox,
        CanonicalFault::BeforeReceipt,
    ] {
        assert!(repository
            .commit_with_fault(request.clone(), fault)
            .await
            .is_err());
        assert_eq!(
            repository.epoch("named-a").await.expect("rollback epoch"),
            request.plan.observed_epoch
        );
        assert!(crate::EntityRepository::new(client.clone())
            .get("named-a", &first.base.id)
            .await
            .expect("first")
            .is_none());
        assert!(crate::EntityRepository::new(client.clone())
            .get("named-a", &second.base.id)
            .await
            .expect("second")
            .is_none());
        assert!(CanonicalRecord::load(&client, "named-a", &mapping.base.id)
            .await
            .expect("mapping state")
            .is_none());
        assert!(CanonicalRecord::load(&client, "named-a", &job.base.id)
            .await
            .expect("job state")
            .is_none());
        assert!(crate::canonical_extension::load_extension_record(
            &client,
            "named-a",
            &extension_id
        )
        .await
        .expect("extension state")
        .is_none());
        assert!(crate::SystemEventRepository::new(client.clone())
            .get("named-a", &event.base.id)
            .await
            .expect("outbox")
            .is_none());
        assert!(repository
            .receipt("named-a", "atomic-regression")
            .await
            .expect("receipt")
            .is_none());
    }
    let receipt = repository
        .commit(request.clone())
        .await
        .expect("whole publication");
    assert_eq!(receipt.effect_ids.len(), 4);
    assert_eq!(receipt.extension_ids, vec![extension_id.clone()]);
    assert_eq!(
        crate::canonical_extension::load_extension_record(&client, "named-a", &extension_id)
            .await
            .expect("extension")
            .as_ref(),
        Some(&extension)
    );
    let loaded = CanonicalRecord::load(&client, "named-a", &mapping.base.id)
        .await
        .expect("mapping native roundtrip")
        .expect("mapping");
    assert_eq!(
        loaded.fingerprint().expect("loaded"),
        CanonicalRecord::IdMap(Box::new(mapping))
            .fingerprint()
            .expect("original")
    );
    let loaded = CanonicalRecord::load(&client, "named-a", &job.base.id)
        .await
        .expect("job native roundtrip")
        .expect("job");
    assert_eq!(
        loaded.fingerprint().expect("loaded"),
        CanonicalRecord::ReprocessJob(Box::new(job))
            .fingerprint()
            .expect("original")
    );
    assert_eq!(receipt.outbox_ids, vec![event.base.id.clone()]);
    assert_eq!(
        repository
            .commit(request.clone())
            .await
            .expect("exact receipt replay"),
        receipt
    );
    let mut altered = request.clone();
    if let Some(CanonicalRecord::Entity(row)) = &mut altered.plan.effects[2].after {
        row.display_name = "changed under old caller hash".into();
    }
    assert!(
        repository.commit(altered).await.is_err(),
        "caller hash cannot conceal altered semantic plan"
    );
    assert!(crate::EntityRepository::new(client.clone())
        .get("named-a", &first.base.id)
        .await
        .expect("committed entity")
        .is_some());
    assert!(crate::EntityRepository::new(client)
        .get("named-b", &first.base.id)
        .await
        .expect("foreign entity")
        .is_none());
}

#[tokio::test]
async fn embedded_atomic_bundle_faults_replay_and_space_isolation() {
    atomic_effects_outbox_receipt(true).await;
}
#[tokio::test]
async fn surreal_atomic_bundle_faults_replay_and_space_isolation() {
    atomic_effects_outbox_receipt(false).await;
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
#[expect(
    clippy::too_many_lines,
    reason = "Keep the complete ordered conformance scenario and its failure assertions together."
)]
async fn decision_inverse_restores_independent_baseline(embedded: bool) {
    use crate::source_decision::SourceDecisionRepository;
    use elephant_types::records::SourceType;
    let client = client(embedded).await;
    let source = crate::SourceRepository::new(client.clone())
        .create(Source::new(
            BaseFields::new(
                RecordId::new_v4(TableName::Source),
                "named-a".into(),
                "test".into(),
            ),
            SourceType::Manual,
            "source-b-content".into(),
        ))
        .await
        .expect("legacy supported source");
    let baseline = crate::EntityRepository::new(client.clone())
        .create(entity("independently supported claim"))
        .await
        .expect("baseline");
    let mut hidden = baseline.clone();
    hidden.base.deleted_at = Some(Utc::now());
    hidden.base.updated_at = Utc::now();
    let repository = CanonicalRepository::new(client.clone());
    let receipt = repository
        .commit(CanonicalCommit {
            space_id: "named-a".into(),
            commit_id: "cross-source-decision".into(),
            input_fingerprint: "b-supersedes-baseline".into(),
            replay_output: Some(serde_json::json!({"pending":["derived-successor"]})),
            publication: None,
            finalization: None,
            maintenance: Some(CanonicalMaintenanceContext {
                origin: CanonicalMaintenanceOrigin::Derived,
                attempt: None,
                sources: vec![PublishedSourceFence::Legacy {
                    source: Box::new(source.clone()),
                }],
            }),
            plan: CanonicalPlan {
                extension_effects: Vec::new(),
                observed_epoch: repository.epoch("named-a").await.expect("epoch"),
                dependencies: vec![CanonicalRecord::Source(Box::new(source.clone()))],
                effects: vec![CanonicalEffect {
                    before: Some(CanonicalRecord::Entity(Box::new(baseline.clone()))),
                    after: Some(CanonicalRecord::Entity(Box::new(hidden))),
                }],
                outbox: vec![],
            },
        })
        .await
        .expect("atomic decision");
    assert_eq!(receipt.source_ids, vec![source.base.id.clone()]);
    let source_authority = SourcePublicationRepository::new(client.clone());
    assert!(
        source_authority
            .support_for_record("named-a", &baseline.base.id)
            .await
            .unwrap()
            .is_empty(),
        "legacy decisions have causal receipts without a publication ledger"
    );
    assert!(source_authority
        .has_causal_source_support("named-a", &baseline.base.id)
        .await
        .unwrap());
    assert!(!source_authority
        .has_causal_source_support("foreign-space", &baseline.base.id)
        .await
        .unwrap());
    assert!(!source_authority
        .has_causal_source_support("named-a", &RecordId::new_v4(TableName::Entity))
        .await
        .unwrap());
    assert_eq!(
        receipt.replay_output,
        Some(serde_json::json!({"pending":["derived-successor"]}))
    );
    let inverse = SourceDecisionRepository::new(client.clone())
        .invalidation("named-a", &source.base.id)
        .await
        .expect("inverse plan");
    assert_eq!(inverse.effects.len(), 1);
    let restored = inverse.effects[0]
        .after
        .as_ref()
        .expect("preserved baseline");
    assert_eq!(restored.base().id, baseline.base.id);
    assert_eq!(
        restored.base().deleted_at,
        None,
        "withdrawn decision must not leave baseline hidden"
    );
    assert_eq!(inverse.decision_keys.len(), 1);
    assert_eq!(
        inverse.receipt_keys.len(),
        1,
        "successor replay output must be erased too"
    );
    assert_eq!(
        repository.epoch("named-a").await.expect("unchanged epoch"),
        inverse.observed_epoch,
        "preparation must not mutate live state"
    );
    crate::SourceRepository::new(client)
        .delete("named-a", &source.base.id)
        .await
        .expect("withdraw source");
    assert!(
        repository
            .receipt("named-a", "cross-source-decision")
            .await
            .is_err(),
        "withdrawn source must block private successor payload replay"
    );
}

#[tokio::test]
async fn embedded_decision_invalidation_restores_surviving_baseline() {
    decision_inverse_restores_independent_baseline(true).await;
}
#[tokio::test]
async fn surreal_decision_invalidation_restores_surviving_baseline() {
    decision_inverse_restores_independent_baseline(false).await;
}

#[test]
fn source_support_proof_binds_semantics_but_ignores_bookkeeping_time() {
    let mut row = entity("verified semantic content");
    let verified = source_support_fingerprint(&row).expect("proof");
    row.base.updated_at += chrono::Duration::seconds(1);
    assert_eq!(
        source_support_fingerprint(&row).expect("bookkeeping"),
        verified
    );
    row.display_name = "unverified direct change".into();
    assert_ne!(
        source_support_fingerprint(&row).expect("changed content"),
        verified
    );
    assert_eq!(
        CanonicalRecord::Entity(Box::new(row.clone()))
            .fingerprint()
            .expect("enum proof"),
        source_support_fingerprint(&row).expect("domain proof")
    );
}

fn extension_row(id: &surrealdb::sql::Thing, owner: &str, note: Option<&str>) -> Value {
    use std::collections::BTreeMap;
    let mut metadata = BTreeMap::<String, Value>::from([("owner".into(), owner.into())]);
    if let Some(note) = note {
        metadata.insert("independent_note".into(), note.into());
    }
    Value::Object(surrealdb::sql::Object::from(
        BTreeMap::<String, Value>::from([
            ("id".into(), Value::Thing(id.clone())),
            ("space_id".into(), "named-a".into()),
            (
                "metadata".into(),
                Value::Object(surrealdb::sql::Object::from(metadata)),
            ),
        ]),
    ))
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
#[expect(
    clippy::too_many_lines,
    reason = "Keep the complete ordered conformance scenario and its failure assertions together."
)]
async fn extension_inverse_preserves_independent_fields(embedded: bool) {
    use crate::canonical_extension::{load_extension_record, CanonicalExtensionEffect};
    let client = client(embedded).await;
    let repo = CanonicalRepository::new(client.clone());
    let source = crate::SourceRepository::new(client.clone())
        .create(Source::new(
            BaseFields::new(
                RecordId::new_v4(TableName::Source),
                "named-a".into(),
                "test".into(),
            ),
            elephant_types::records::SourceType::Manual,
            "causal input".into(),
        ))
        .await
        .expect("source");
    ensure_extension_epoch_guards(&client, &["test_merge_metadata".into()])
        .await
        .expect("guards");
    let id = surrealdb::sql::Thing::from(("test_merge_metadata", "inverse"));
    let baseline = extension_row(&id, "independent baseline", None);
    let derived = extension_row(&id, "source-derived owner", None);
    let independent = extension_row(
        &id,
        "source-derived owner",
        Some("keep this independent note"),
    );
    let source_authority = PublishedSourceFence::Legacy {
        source: Box::new(source.clone()),
    };
    for (key, before, after, origin) in [
        ("extension-baseline", None, baseline.clone(), None),
        (
            "extension-derived",
            Some(baseline.clone()),
            derived.clone(),
            Some(CanonicalMaintenanceOrigin::Derived),
        ),
        (
            "extension-independent-edit",
            Some(derived.clone()),
            independent.clone(),
            Some(CanonicalMaintenanceOrigin::IndependentMutation),
        ),
    ] {
        let plan = CanonicalPlan {
            observed_epoch: repo.epoch("named-a").await.expect("epoch"),
            extension_effects: vec![CanonicalExtensionEffect {
                id: id.clone(),
                before,
                after: Some(after),
            }],
            ..CanonicalPlan::default()
        };
        let maintenance = origin.map(|origin| CanonicalMaintenanceContext {
            origin,
            attempt: None,
            sources: vec![source_authority.clone()],
        });
        repo.commit(CanonicalCommit {
            space_id: "named-a".into(),
            commit_id: key.into(),
            input_fingerprint: key.into(),
            replay_output: None,
            publication: None,
            maintenance,
            finalization: None,
            plan,
        })
        .await
        .expect("canonical extension decision");
    }
    let inverse = crate::source_decision::SourceDecisionRepository::new(client.clone())
        .invalidation("named-a", &source.base.id)
        .await
        .expect("inverse");
    assert_eq!(inverse.extension_effects.len(), 1);
    assert_eq!(
        inverse.extension_effects[0].before,
        Some(independent.clone())
    );
    assert_eq!(
        inverse.extension_effects[0].after,
        Some(extension_row(
            &id,
            "independent baseline",
            Some("keep this independent note")
        ))
    );
    assert_eq!(
        load_extension_record(&client, "named-a", &id)
            .await
            .expect("unchanged during planning"),
        Some(independent)
    );
    assert_eq!(
        inverse.decision_keys.len(),
        1,
        "independently authored edits must not be invalidated as source decisions"
    );
}

#[tokio::test]
async fn embedded_extension_inverse_preserves_independent_nested_fields() {
    extension_inverse_preserves_independent_fields(true).await;
}
#[tokio::test]
async fn surreal_extension_inverse_preserves_independent_nested_fields() {
    extension_inverse_preserves_independent_fields(false).await;
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
async fn canonical_row_cas_uses_stored_nested_shape(embedded: bool) {
    let client = client(embedded).await;
    let repository = CanonicalRepository::new(client.clone());
    let original = crate::EntityRepository::new(client.clone())
        .create(entity("before"))
        .await
        .expect("persisted entity with absent optional provenance/security fields");
    let mut updated = original.clone();
    updated.display_name = "after".into();
    updated.base.updated_at = Utc::now();
    let mut request = CanonicalCommit {
        space_id: "named-a".into(),
        commit_id: "persisted-shape-cas".into(),
        input_fingerprint: "persisted-shape-cas".into(),
        replay_output: None,
        publication: None,
        maintenance: None,
        finalization: None,
        plan: CanonicalPlan {
            observed_epoch: repository.epoch("named-a").await.expect("epoch"),
            dependencies: vec![],
            effects: vec![CanonicalEffect {
                before: Some(CanonicalRecord::Entity(Box::new(original.clone()))),
                after: Some(CanonicalRecord::Entity(Box::new(updated.clone()))),
            }],
            extension_effects: vec![],
            outbox: vec![],
        },
    };
    repository
        .commit(request.clone())
        .await
        .expect("matching persisted row");
    let entities = crate::EntityRepository::new(client.clone());
    assert_eq!(
        entities
            .get("named-a", &original.base.id)
            .await
            .expect("current"),
        Some(updated.clone())
    );
    // Refresh only the predicate fence: an old before-image must still fail.
    request.commit_id = "stale-row-current-epoch".into();
    request.plan.observed_epoch = repository.epoch("named-a").await.expect("current epoch");
    assert!(repository.commit(request).await.is_err());
    assert_eq!(
        entities
            .get("named-a", &original.base.id)
            .await
            .expect("current"),
        Some(updated)
    );
    assert!(repository
        .receipt("named-a", "stale-row-current-epoch")
        .await
        .expect("receipt read")
        .is_none());
}

#[tokio::test]
async fn embedded_canonical_row_cas_uses_stored_nested_shape() {
    canonical_row_cas_uses_stored_nested_shape(true).await;
}

#[tokio::test]
async fn surreal_canonical_row_cas_uses_stored_nested_shape() {
    canonical_row_cas_uses_stored_nested_shape(false).await;
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
async fn canonical_relationship_endpoint_move_is_atomic(embedded: bool) {
    let client = client(embedded).await;
    let entities = crate::EntityRepository::new(client.clone());
    let old_subject = entities
        .create(entity("old subject"))
        .await
        .expect("old subject");
    let new_subject = entities
        .create(entity("new subject"))
        .await
        .expect("new subject");
    let object = entities.create(entity("object")).await.expect("object");
    let relations = crate::RelRepository::new(client.clone());
    let original = relations
        .create(Rel {
            base: BaseFields::new(
                RecordId::new_v4(TableName::Rel),
                "named-a".into(),
                "test".into(),
            ),
            subject: elephant_types::Link::new(old_subject.base.id.clone()),
            object: elephant_types::Link::new(object.base.id.clone()),
            rel_type: "works_at".into(),
            status: elephant_types::records::ClaimStatus::Approved,
            confidence: 0.95,
            validity_window: None,
            evidence: vec![],
            temporal_audit: None,
        })
        .await
        .expect("original edge");
    let mut moved = original.clone();
    moved.subject = elephant_types::Link::new(new_subject.base.id.clone());
    moved.base.updated_at = Utc::now();
    let repository = CanonicalRepository::new(client.clone());
    let request = CanonicalCommit {
        space_id: "named-a".into(),
        commit_id: "move-native-endpoints".into(),
        input_fingerprint: "merge-original-authority".into(),
        replay_output: None,
        publication: None,
        maintenance: None,
        finalization: None,
        plan: CanonicalPlan {
            observed_epoch: repository.epoch("named-a").await.expect("epoch"),
            dependencies: vec![],
            extension_effects: vec![],
            outbox: vec![],
            effects: vec![CanonicalEffect {
                before: Some(CanonicalRecord::Rel(Box::new(original.clone()))),
                after: Some(CanonicalRecord::Rel(Box::new(moved.clone()))),
            }],
        },
    };
    assert!(repository
        .commit_with_fault(request.clone(), CanonicalFault::AfterFirstEffect)
        .await
        .is_err());
    assert_eq!(
        relations
            .get("named-a", &original.base.id)
            .await
            .expect("rolled back edge"),
        Some(original.clone())
    );
    assert!(relations
        .get_entities_within_n_hops("named-a", &old_subject.base.id, 1)
        .await
        .expect("old graph")
        .iter()
        .any(|neighbor| neighbor.entity_id == object.base.id));
    repository
        .commit(request)
        .await
        .expect("atomic endpoint move");
    assert_eq!(
        relations
            .get("named-a", &original.base.id)
            .await
            .expect("same edge identity"),
        Some(moved)
    );
    assert!(relations
        .get_entities_within_n_hops("named-a", &old_subject.base.id, 1)
        .await
        .expect("old graph emptied")
        .iter()
        .all(|neighbor| neighbor.entity_id != object.base.id));
    assert!(relations
        .get_entities_within_n_hops("named-a", &new_subject.base.id, 1)
        .await
        .expect("new graph")
        .iter()
        .any(|neighbor| neighbor.entity_id == object.base.id));
}

#[tokio::test]
async fn embedded_canonical_relationship_endpoint_move_is_atomic() {
    canonical_relationship_endpoint_move_is_atomic(true).await;
}

#[tokio::test]
async fn surreal_canonical_relationship_endpoint_move_is_atomic() {
    canonical_relationship_endpoint_move_is_atomic(false).await;
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
#[expect(
    clippy::too_many_lines,
    reason = "Keep the complete ordered conformance scenario and its failure assertions together."
)]
async fn independent_edits_cannot_relabel_source_facts(embedded: bool) {
    let client = client(embedded).await;
    let source = crate::SourceRepository::new(client.clone())
        .create(Source::new(
            BaseFields::new(
                RecordId::new_v4(TableName::Source),
                "named-a".into(),
                "test".into(),
            ),
            elephant_types::records::SourceType::Manual,
            "legacy-source".into(),
        ))
        .await
        .expect("legacy source");
    let subject = crate::EntityRepository::new(client.clone())
        .create(entity("subject"))
        .await
        .expect("subject");
    let object = crate::EntityRepository::new(client.clone())
        .create(entity("object"))
        .await
        .expect("object");
    let mut base = BaseFields::new(
        RecordId::new_v4(TableName::Rel),
        "named-a".into(),
        "test".into(),
    );
    base.provenance.sources = vec![source.base.id.clone()];
    // Retained source-attributed legacy candidates need no manufactured new
    // publication receipt for this guard. Their old lineage is still not a
    // certificate for an independently changed factual tuple.
    let before = crate::RelRepository::new(client.clone())
        .create(Rel {
            base,
            subject: elephant_types::Link::new(subject.base.id),
            object: elephant_types::Link::new(object.base.id),
            rel_type: "opened".into(),
            status: ClaimStatus::Candidate,
            confidence: 0.2,
            validity_window: None,
            evidence: vec![],
            temporal_audit: None,
        })
        .await
        .expect("legacy source claim");
    let repository = CanonicalRepository::new(client.clone());
    let epoch = repository.epoch("named-a").await.expect("epoch");
    let event = SystemEvent {
        base: BaseFields::new(
            RecordId::new_v4(TableName::SystemEvent),
            "named-a".into(),
            "test".into(),
        ),
        seq: 0,
        event_type: "records.changed".into(),
        payload: serde_json::json!({"id":before.base.id}),
    };
    let request_for = |after: Rel, key: &str| CanonicalCommit {
        space_id: "named-a".into(),
        commit_id: key.into(),
        input_fingerprint: key.into(),
        replay_output: None,
        publication: None,
        finalization: None,
        maintenance: Some(CanonicalMaintenanceContext {
            origin: CanonicalMaintenanceOrigin::IndependentMutation,
            attempt: None,
            sources: vec![PublishedSourceFence::Legacy {
                source: Box::new(source.clone()),
            }],
        }),
        plan: CanonicalPlan {
            observed_epoch: epoch.clone(),
            dependencies: vec![CanonicalRecord::Source(Box::new(source.clone()))],
            effects: vec![CanonicalEffect {
                before: Some(CanonicalRecord::Rel(Box::new(before.clone()))),
                after: Some(CanonicalRecord::Rel(Box::new(after))),
            }],
            outbox: vec![event.clone()],
            ..CanonicalPlan::default()
        },
    };
    let mut changed = before.clone();
    changed.rel_type = "destroyed".into();
    changed.status = ClaimStatus::Approved;
    assert!(repository
        .commit(request_for(changed.clone(), "changed-source-fact"))
        .await
        .is_err());
    changed.base.provenance.sources.clear();
    assert!(
        repository
            .commit(request_for(changed, "stripped-source-fact"))
            .await
            .is_err(),
        "removing the source link cannot launder the old fact"
    );
    assert_eq!(
        crate::RelRepository::new(client.clone())
            .get("named-a", &before.base.id)
            .await
            .expect("unchanged"),
        Some(before.clone())
    );
    assert!(crate::SystemEventRepository::new(client.clone())
        .get("named-a", &event.base.id)
        .await
        .expect("no outbox")
        .is_none());
    assert!(repository
        .receipt("named-a", "changed-source-fact")
        .await
        .expect("no receipt")
        .is_none());
    let mut annotation = before.clone();
    annotation.confidence = 0.8;
    annotation.status = ClaimStatus::Disputed;
    repository
        .commit(request_for(annotation.clone(), "annotate-source-fact"))
        .await
        .expect("status and confidence correction remains available");
    assert_eq!(
        crate::RelRepository::new(client.clone())
            .get("named-a", &before.base.id)
            .await
            .expect("corrected"),
        Some(annotation)
    );
}

#[tokio::test]
async fn embedded_independent_factual_edits_cannot_refresh_old_source_credit() {
    independent_edits_cannot_relabel_source_facts(true).await;
}
#[tokio::test]
async fn surreal_independent_factual_edits_cannot_refresh_old_source_credit() {
    independent_edits_cannot_relabel_source_facts(false).await;
}

#[test]
fn copied_assertion_identity_or_recency_is_not_new_documentary_evidence() {
    let span = elephant_types::records::EvidenceSpan {
        revision: elephant_types::Link::new(RecordId::new_v4(TableName::DocRevision)),
        node: None,
        start: 0,
        end: 10,
        quote_hash: "same immutable passage".into(),
    };
    let mut before = Rel {
        base: BaseFields::new(
            RecordId::new_v4(TableName::Rel),
            "named-a".into(),
            "test".into(),
        ),
        subject: elephant_types::Link::new(RecordId::new_v4(TableName::Entity)),
        object: elephant_types::Link::new(RecordId::new_v4(TableName::Entity)),
        rel_type: "opened".into(),
        status: ClaimStatus::Approved,
        confidence: 0.8,
        validity_window: None,
        evidence: vec![elephant_types::records::rel::EvidenceRef {
            assertion: elephant_types::Link::new(RecordId::new_v4(TableName::Assertion)),
            span,
            time_basis: elephant_types::records::rel::TimeBasis::ObservedAt,
            time_value: Utc::now(),
        }],
        temporal_audit: None,
    };
    let proof =
        documentary_proofs(&CanonicalRecord::Rel(Box::new(before.clone()))).expect("original");
    before.evidence[0].assertion =
        elephant_types::Link::new(RecordId::new_v4(TableName::Assertion));
    before.evidence[0].time_value += chrono::Duration::days(1);
    assert_eq!(
        proof,
        documentary_proofs(&CanonicalRecord::Rel(Box::new(before))).expect("copied")
    );
}

#[test]
fn original_queue_context_is_a_read_dependency_and_never_a_canonical_effect() {
    let item = WorkItem {
        base: BaseFields::new(
            RecordId::new_v4(TableName::WorkItem),
            "named-a".into(),
            "test".into(),
        ),
        kind: "trusted-proposal-context".into(),
        payload: serde_json::json!({"provenance_only":true}),
        status: WorkItemStatus::Done,
        priority: 0,
        idempotency_key: "original-context".into(),
        attempts: 0,
        lease_expires_at: None,
        leased_by: None,
        run_after: None,
        last_error: None,
    };
    let run = WorkRun {
        base: BaseFields::new(
            RecordId::new_v4(TableName::WorkRun),
            "named-a".into(),
            "test".into(),
        ),
        work_item: elephant_types::Link::new(item.base.id.clone()),
        attempt: 0,
        started_at: Utc::now(),
        completed_at: Some(Utc::now()),
        success: true,
        error: None,
        metrics: elephant_types::records::WorkRunMetrics::default(),
        worker_id: "trusted-proposal-context".into(),
        worker_version: "test".into(),
        model_version: None,
        prompt_id: None,
        config_hash: None,
    };
    for record in [
        CanonicalRecord::WorkItem(Box::new(item)),
        CanonicalRecord::WorkRun(Box::new(run)),
    ] {
        let mut request = CanonicalCommit {
            space_id: "named-a".into(),
            commit_id: "read-original-context".into(),
            input_fingerprint: "original".into(),
            replay_output: None,
            publication: None,
            maintenance: None,
            finalization: None,
            plan: CanonicalPlan {
                observed_epoch: "observed".into(),
                dependencies: vec![record.clone()],
                effects: vec![],
                extension_effects: vec![],
                outbox: vec![],
            },
        };
        validate_commit(&request).expect("exact read dependency");
        request.plan.effects.push(CanonicalEffect {
            before: None,
            after: Some(record.clone()),
        });
        assert!(validate_commit(&request).is_err());
        request.plan.effects[0] = CanonicalEffect {
            before: Some(record.clone()),
            after: Some(record.clone()),
        };
        assert!(validate_commit(&request).is_err());
        request.plan.effects[0] = CanonicalEffect {
            before: Some(record),
            after: None,
        };
        assert!(validate_commit(&request).is_err());
    }
}

#[test]
fn support_fingerprint_ignores_only_object_order_and_update_time() {
    let original: serde_json::Value = serde_json::from_str(r#"{"updated_at":"before","before":{"z":null,"a":{"right":2,"left":1}},"after":[{"b":false,"a":"value"},1,2],"confidence":0.0}"#).expect("original");
    let reordered: serde_json::Value = serde_json::from_str(r#"{"confidence":0.0,"after":[{"a":"value","b":false},1,2],"before":{"a":{"left":1,"right":2},"z":null},"updated_at":"later"}"#).expect("native object order");
    let fingerprint = source_support_fingerprint(&original).expect("fingerprint");
    assert_eq!(
        fingerprint,
        source_support_fingerprint(&reordered).expect("reordered")
    );
    let mut array_changed = reordered.clone();
    array_changed["after"]
        .as_array_mut()
        .expect("array")
        .swap(1, 2);
    assert_ne!(
        fingerprint,
        source_support_fingerprint(&array_changed).expect("ordered array")
    );
    let mut scalar_changed = reordered.clone();
    scalar_changed["before"]["z"] = "null".into();
    assert_ne!(
        fingerprint,
        source_support_fingerprint(&scalar_changed).expect("null is not text")
    );
    let mut absent_changed = reordered.clone();
    absent_changed["before"]
        .as_object_mut()
        .expect("object")
        .remove("z");
    assert_ne!(
        fingerprint,
        source_support_fingerprint(&absent_changed).expect("missing is not null")
    );
    let mut number_changed = reordered;
    number_changed["confidence"] = serde_json::json!(0);
    assert_ne!(
        fingerprint,
        source_support_fingerprint(&number_changed).expect("integer representation is distinct")
    );
}

#[test]
fn source_input_is_not_its_own_derived_support() {
    let mut source = Source::new(
        BaseFields::new(
            RecordId::new_v4(TableName::Source),
            "named-a".into(),
            "fixture".into(),
        ),
        elephant_types::records::SourceType::Manual,
        "input-a".into(),
    );
    // Even accidental self-provenance cannot change input authority into a
    // derived proof that later accepted input must preserve byte-for-byte.
    source.base.provenance.sources.push(source.base.id.clone());
    let mut completed = source.clone();
    completed.current_ingest_mode = Some(elephant_types::records::ingest::IngestMode::Metadata);
    completed.lifecycle_status =
        elephant_types::records::source::SourceLifecycleStatus::MetadataReady;
    let effect = CanonicalEffect {
        before: Some(CanonicalRecord::Source(Box::new(source.clone()))),
        after: Some(CanonicalRecord::Source(Box::new(completed))),
    };
    assert!(!effect_attributes_source(&effect, &source.base.id));
    let mut derived = entity("real derived contribution");
    derived.base.provenance.sources.push(source.base.id.clone());
    assert!(effect_attributes_source(
        &CanonicalEffect {
            before: None,
            after: Some(CanonicalRecord::Entity(Box::new(derived)))
        },
        &source.base.id
    ));
    let other = RecordId::new_v4(TableName::Source);
    source.base.provenance.sources.push(other.clone());
    assert!(effect_attributes_source(
        &CanonicalEffect {
            before: None,
            after: Some(CanonicalRecord::Source(Box::new(source)))
        },
        &other
    ));
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
#[expect(
    clippy::too_many_lines,
    reason = "Keep the complete ordered conformance scenario and its failure assertions together."
)]
async fn pending_source_allows_only_original_job_controls(embedded: bool) {
    use crate::source_publication::AcceptSourceRevision;
    use elephant_types::publication::SourcePrecondition;
    use elephant_types::records::reprocess::ReprocessStatus;
    let client = client(embedded).await;
    let publication = SourcePublicationRepository::new(client.clone());
    let source = Source::new(
        BaseFields::new(
            RecordId::new_v4(TableName::Source),
            "named-a".into(),
            "test".into(),
        ),
        SourceType::Manual,
        "pending job input".into(),
    );
    let accepted = publication
        .accept(AcceptSourceRevision {
            source: source.clone(),
            revision_id: "control-input".into(),
            expected: SourcePrecondition::Absent,
            idempotency_key: "control-source".into(),
            request_fingerprint: "control-input".into(),
            reactivate: false,
            recreate_after_erasure: false,
            authorized: None,
            authorized_document: None,
        })
        .await
        .unwrap();
    assert!(accepted.published_revision.is_none());
    let mut base = BaseFields::new(
        RecordId::new_v4(TableName::ReprocessJob),
        "named-a".into(),
        "test".into(),
    );
    base.provenance.sources = vec![source.base.id.clone()];
    let job = crate::ReprocessJobRepository::new(client.clone())
        .create(ReprocessJob {
            base,
            name: "bounded pending input control".into(),
            selection_criteria: elephant_types::records::SelectionCriteria::default(),
            target_stages: elephant_types::records::reprocess::default_reprocess_target_stages(),
            status: ReprocessStatus::Running,
            progress: elephant_types::records::reprocess::ReprocessProgress::default(),
            priority: 0,
            budget_limit_euros: Some(1.0),
            max_sources_per_day: None,
            started_at: Some(Utc::now()),
            completed_at: None,
            error: None,
            reason: None,
        })
        .await
        .unwrap();
    let canonical = CanonicalRepository::new(client.clone());
    assert!(canonical
        .capture_maintenance_support(
            "named-a",
            &[CanonicalRecord::ReprocessJob(Box::new(job.clone()))]
        )
        .await
        .is_err());
    let epoch = canonical.epoch("named-a").await.unwrap();
    let sources = canonical
        .capture_reprocess_control_support("named-a", &job)
        .await
        .unwrap();
    assert!(
        matches!(&sources[..], [PublishedSourceFence::PendingControl { head, .. }] if head.published_revision.is_none())
    );
    let mut after = job.clone();
    after.status = ReprocessStatus::Paused;
    after.error = Some("unpriced dispatch refused".into());
    let mut audit_base = job.base.clone();
    audit_base.id = RecordId::new_v4(TableName::AuditLog);
    let audit = AuditLog {
        base: audit_base,
        action: AuditAction::Update,
        actor: "test".into(),
        record: job.base.id.clone(),
        before: Some(serde_json::to_value(&job).unwrap()),
        after: Some(serde_json::to_value(&after).unwrap()),
        timestamp: Utc::now(),
        reason: Some("budget refusal".into()),
    };
    let mut event_base = job.base.clone();
    event_base.id = RecordId::new_v4(TableName::SystemEvent);
    let request = CanonicalCommit {
        space_id: "named-a".into(),
        commit_id: "pending-job-control".into(),
        input_fingerprint: "pending-job-control".into(),
        replay_output: None,
        publication: None,
        finalization: None,
        maintenance: Some(CanonicalMaintenanceContext {
            origin: CanonicalMaintenanceOrigin::ControlOnly,
            attempt: None,
            sources,
        }),
        plan: CanonicalPlan {
            observed_epoch: epoch,
            dependencies: vec![],
            extension_effects: vec![],
            effects: vec![
                CanonicalEffect {
                    before: Some(CanonicalRecord::ReprocessJob(Box::new(job.clone()))),
                    after: Some(CanonicalRecord::ReprocessJob(Box::new(after))),
                },
                CanonicalEffect {
                    before: None,
                    after: Some(CanonicalRecord::AuditLog(Box::new(audit))),
                },
            ],
            outbox: vec![SystemEvent {
                base: event_base,
                seq: 0,
                event_type: "records.changed".into(),
                payload: serde_json::json!({"records":[{"id":job.base.id,"op":"update"}]}),
            }],
        },
    };
    for mutation in ["graph", "budget", "provenance", "derived"] {
        let mut invalid = request.clone();
        match mutation {
            "graph" => invalid.plan.effects.push(CanonicalEffect {
                before: None,
                after: Some(CanonicalRecord::Entity(Box::new(entity(
                    "must not publish",
                )))),
            }),
            "budget" => {
                if let Some(CanonicalRecord::ReprocessJob(job)) = &mut invalid.plan.effects[0].after
                {
                    job.budget_limit_euros = None;
                }
            }
            "provenance" => {
                if let Some(CanonicalRecord::ReprocessJob(job)) = &mut invalid.plan.effects[0].after
                {
                    job.base.provenance.sources.clear();
                }
            }
            _ => invalid.maintenance.as_mut().unwrap().origin = CanonicalMaintenanceOrigin::Derived,
        }
        assert!(
            canonical.commit(invalid).await.is_err(),
            "invalid {mutation} control must refuse"
        );
        assert!(canonical
            .receipt("named-a", &request.commit_id)
            .await
            .unwrap()
            .is_none());
    }
    let receipt = canonical.commit(request.clone()).await.unwrap();
    assert_eq!(receipt.effect_ids.len(), 2);
    assert_eq!(
        canonical.commit(request).await.unwrap().commit_id,
        receipt.commit_id
    );
    assert!(publication
        .support_for_record("named-a", &job.base.id)
        .await
        .unwrap()
        .is_empty());
    let head = publication
        .head("named-a", &source.base.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(head.generation, accepted.source_generation);
    assert!(
        head.published_revision.is_none(),
        "job bookkeeping cannot publish its input"
    );
    let jobs = crate::ReprocessJobRepository::new(client.clone());
    assert_eq!(
        jobs.get("named-a", &job.base.id)
            .await
            .unwrap()
            .unwrap()
            .status,
        ReprocessStatus::Paused
    );
}
#[tokio::test]
#[expect(
    clippy::large_futures,
    reason = "Preserve the original future construction and polling shape exercised by this regression."
)]
async fn embedded_pending_source_control_cannot_publish_graph() {
    pending_source_allows_only_original_job_controls(true).await;
}
#[tokio::test]
#[expect(
    clippy::large_futures,
    reason = "Preserve the original future construction and polling shape exercised by this regression."
)]
async fn surreal_pending_source_control_cannot_publish_graph() {
    pending_source_allows_only_original_job_controls(false).await;
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
async fn readonly_epoch_tracks_real_writers_without_initializing_missing_scope(embedded: bool) {
    let client = client(embedded).await;
    let repository = CanonicalRepository::new(client.clone());
    let initialized = repository.epoch("named-a").await.unwrap();
    assert_eq!(
        repository.current_epoch("named-a").await.unwrap(),
        initialized
    );
    assert!(repository.current_epoch("no-such-space").await.is_err());
    if let Some(store) = client.embedded_store() {
        assert!(crate::private_control_read::row(
            &store,
            "canonical_epoch",
            "no-such-space",
            "no-such-space"
        )
        .await
        .unwrap()
        .is_none());
    } else {
        let mut response = client
            .query_bind_checked(
                "SELECT * FROM ONLY $id;",
                crate::bindings! {"id" => control_thing("canonical_epoch", "no-such-space")},
            )
            .await
            .unwrap();
        let missing: Option<serde_json::Value> = response.take(0).unwrap();
        assert!(missing.is_none());
    }
    crate::EntityRepository::new(client)
        .create(entity("actual epoch writer"))
        .await
        .unwrap();
    let after = repository.current_epoch("named-a").await.unwrap();
    assert_ne!(after, initialized);
    assert_eq!(repository.current_epoch("named-a").await.unwrap(), after);
}

#[tokio::test]
async fn embedded_readonly_canonical_epoch_never_initializes_missing_scope() {
    readonly_epoch_tracks_real_writers_without_initializing_missing_scope(true).await;
}
#[tokio::test]
async fn surreal_readonly_canonical_epoch_never_initializes_missing_scope() {
    readonly_epoch_tracks_real_writers_without_initializing_missing_scope(false).await;
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
async fn readonly_epoch_absence_is_original_state_not_missing_execution_authority(embedded: bool) {
    let client = client(embedded).await;
    let repository = CanonicalRepository::new(client.clone());
    let absent = repository
        .observe_epoch("new-configured-space")
        .await
        .unwrap();
    assert_eq!(absent, None);
    assert_eq!(
        repository
            .observe_epoch("new-configured-space")
            .await
            .unwrap(),
        absent
    );
    let config = SpaceConfig {
        base: BaseFields::new(
            RecordId::new_v4(TableName::SpaceConfig),
            "new-configured-space".into(),
            "fixture".into(),
        ),
        embedding_provider: "fake".into(),
        embedding_model: "no-provider".into(),
        embedding_dims: 8,
        processing: None,
        purpose: None,
        skill: None,
    };
    SpaceConfigRepository::new(client.clone())
        .create(config)
        .await
        .unwrap();
    let present = repository
        .observe_epoch("new-configured-space")
        .await
        .unwrap();
    assert!(
        present.is_some(),
        "ordinary SpaceConfig create initializes membership on both backends"
    );
    assert_ne!(
        present, absent,
        "an original missing epoch cannot adopt a newly initialized one"
    );

    let key = "foreign-epoch-physical-key";
    if let Some(store) = client.embedded_store() {
        store.mutate(|tx| {
            tx.upsert_extension_row("canonical_epoch", serde_json::json!({
                "id":format!("canonical_epoch:{key}"), "space_id":"foreign-owner", "epoch":"actual-foreign-epoch"
            }))?;
            Ok(())
        }).await.unwrap();
    } else {
        client
            .query_bind_checked(
                "CREATE $id CONTENT {space_id:'foreign-owner',epoch:'actual-foreign-epoch'};",
                crate::bindings! {"id"=>control_thing("canonical_epoch",key)},
            )
            .await
            .unwrap();
    }
    assert!(
        repository.observe_epoch(key).await.is_err(),
        "a foreign physical row is never an absent token"
    );
}

#[tokio::test]
async fn embedded_readonly_epoch_preserves_original_absence_and_rejects_foreign_row() {
    readonly_epoch_absence_is_original_state_not_missing_execution_authority(true).await;
}
#[tokio::test]
async fn surreal_readonly_epoch_preserves_original_absence_and_rejects_foreign_row() {
    readonly_epoch_absence_is_original_state_not_missing_execution_authority(false).await;
}
