//! Real repository regressions for source lifecycle; both backend variants use
//! identical operations and assertions. No private journal writes stand in for erase.
#![allow(clippy::expect_used, clippy::unwrap_used)]

#[path = "source_lifecycle_sequence_tests.rs"]
mod sequence_tests;

#[cfg(feature = "rocksdb")]
#[path = "source_lifecycle_rocksdb_tests.rs"]
mod persistent_rocksdb;

#[path = "source_lifecycle_batch_tests.rs"]
mod batch_tests;

use super::*;
use crate::source_publication::AcceptSourceRevision;
use crate::{
    EntityRepository, Repository, SourceRepository, SpaceConfigRepository, StorageClient,
    StorageConfig,
};
use elephant_types::publication::SourcePrecondition;
use elephant_types::records::{entity::EntityType, Entity, SourceType, SpaceConfig};

async fn client(embedded: bool) -> StorageClient {
    let client = if embedded {
        StorageClient::new_embedded().await
    } else {
        StorageClient::new(StorageConfig::default()).await
    }
    .expect("backend");
    if !embedded {
        client
            .query_checked(include_str!(
                "../../migrations/sql/031_source_publication.surql"
            ))
            .await
            .expect("schema");
        crate::reference_fence::install_native_reference_guards(
            &client,
            &["consumer_note".into(), "consumer_extension".into()],
        )
        .await
        .expect("private reference schema");
    }
    for space in ["erase-a", "erase-b"] {
        let config: SpaceConfig = serde_json::from_value(serde_json::json!({
            "id":RecordId::new_v4(TableName::SpaceConfig),"space_id":space,
            "created_at":Utc::now(),"updated_at":Utc::now(),"created_by":"test",
            "security":{"policy_id":"default","level":0,"labels":[],"handling":[]},
            "provenance":{},"subjects":[],"subjects_state":"complete",
            "embedding_provider":"fake","embedding_model":"test","embedding_dims":8
        }))
        .expect("space");
        SpaceConfigRepository::new(client.clone())
            .create(config)
            .await
            .expect("space create");
    }
    client
}

fn request(space: &str, external: &str) -> AcceptSourceRevision {
    let mut source = Source::new(
        BaseFields::new(
            RecordId::new_v4(TableName::Source),
            space.into(),
            "test".into(),
        ),
        SourceType::Manual,
        "sha-source-payload".into(),
    );
    source.external_id = Some(external.into());
    source.metadata = serde_json::json!({"_manual_doc":{"text":"private source payload ERASE-ME"}});
    AcceptSourceRevision {
        source,
        revision_id: "r1".into(),
        expected: SourcePrecondition::Absent,
        idempotency_key: format!("accept-{external}"),
        request_fingerprint: format!("input-{external}"),
        reactivate: false,
        recreate_after_erasure: false,
        authorized: None,
        authorized_document: None,
    }
}

async fn mutation(
    repository: &SourcePublicationRepository,
    receipt: &OperationReceipt,
    key: &str,
) -> SourceMutation {
    SourceMutation {
        space_id: receipt.space_id.clone(),
        source_id: receipt.source_id.clone(),
        expected: SourcePrecondition::Generation(receipt.source_generation.clone()),
        idempotency_key: key.into(),
        request_fingerprint: format!("mutation-{key}"),
        actor: "test".into(),
        authorized_document: None,
        authorized: repository
            .authorization_snapshot(&receipt.space_id, &receipt.source_id)
            .await
            .expect("snapshot")
            .expect("source"),
    }
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
async fn stale_and_cross_space_refuse_without_intent(embedded: bool) {
    let client = client(embedded).await;
    let repository = SourcePublicationRepository::new(client.clone());
    let accepted = repository
        .accept(request("erase-a", "cas-source"))
        .await
        .expect("accept");
    let mut stale = mutation(&repository, &accepted, "stale").await;
    stale.expected = SourcePrecondition::Generation("stale-generation".into());
    assert!(repository.erase_live(stale).await.is_err());
    assert!(client
        .erasure_journal()
        .unwrap()
        .intents()
        .unwrap()
        .is_empty());
    let mut wrong_space = mutation(&repository, &accepted, "wrong-space").await;
    wrong_space.space_id = "erase-b".into();
    assert!(repository.erase_live(wrong_space).await.is_err());
    assert!(client
        .erasure_journal()
        .unwrap()
        .intents()
        .unwrap()
        .is_empty());
    let authorized = mutation(&repository, &accepted, "late-reclassification").await;
    let sources = SourceRepository::new(client.clone());
    let mut source = sources
        .get("erase-a", &accepted.source_id)
        .await
        .unwrap()
        .unwrap();
    source.base.security.level = 3;
    sources
        .update("erase-a", &accepted.source_id, source)
        .await
        .expect("trusted reclassification");
    assert!(repository.erase_live(authorized).await.is_err());
    assert!(client
        .erasure_journal()
        .unwrap()
        .intents()
        .unwrap()
        .is_empty());
    assert!(sources
        .get("erase-a", &accepted.source_id)
        .await
        .unwrap()
        .is_some());
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
#[expect(
    clippy::too_many_lines,
    reason = "Keep the complete ordered conformance scenario and its failure assertions together."
)]
async fn erasure_closure_and_fresh_incarnation(embedded: bool) {
    let client = client(embedded).await;
    let repository = SourcePublicationRepository::new(client.clone());
    let original = request("erase-a", "same-upstream");
    let accepted = repository.accept(original.clone()).await.unwrap();
    let other = repository
        .accept(request("erase-b", "same-upstream"))
        .await
        .unwrap();
    let entity = Entity::new(
        BaseFields::new(
            RecordId::new_v4(TableName::Entity),
            "erase-a".into(),
            "test".into(),
        ),
        EntityType::new("project"),
        "independent baseline".into(),
    );
    EntityRepository::new(client.clone())
        .create(entity.clone())
        .await
        .unwrap();
    let mut derived = Entity::new(
        BaseFields::new(
            RecordId::new_v4(TableName::Entity),
            "erase-a".into(),
            "test".into(),
        ),
        EntityType::new("project"),
        "ERASE-ME".into(),
    );
    derived
        .base
        .provenance
        .sources
        .push(accepted.source_id.clone());
    EntityRepository::new(client.clone())
        .create(derived.clone())
        .await
        .unwrap();
    let request = mutation(&repository, &accepted, "erase-source").await;
    let receipt = repository
        .erase_live(request.clone())
        .await
        .expect("complete erase");
    assert_eq!(receipt.state, OperationState::Erased);
    assert!(SourceRepository::new(client.clone())
        .get_with_deleted("erase-a", &accepted.source_id)
        .await
        .unwrap()
        .is_none());
    assert!(repository
        .revision("erase-a", &accepted.source_id, "r1")
        .await
        .unwrap()
        .is_none());
    assert!(EntityRepository::new(client.clone())
        .get_with_deleted("erase-a", &derived.base.id)
        .await
        .unwrap()
        .is_none());
    assert!(EntityRepository::new(client.clone())
        .get("erase-a", &entity.base.id)
        .await
        .unwrap()
        .is_some());
    assert!(SourceRepository::new(client.clone())
        .get("erase-b", &other.source_id)
        .await
        .unwrap()
        .is_some());
    assert!(
        EntityRepository::new(client.clone())
            .create(derived)
            .await
            .is_err(),
        "permanent source reference fence rejects late write"
    );
    assert_eq!(repository.erase_live(request).await.unwrap(), receipt);
    let old_head = repository
        .head("erase-a", &accepted.source_id)
        .await
        .unwrap()
        .unwrap();
    let mut recreated = original;
    recreated.source.base.id = RecordId::new_v4(TableName::Source);
    recreated.revision_id = "r2".into();
    recreated.idempotency_key = "fresh-incarnation".into();
    recreated.request_fingerprint = "fresh-content".into();
    recreated.expected = SourcePrecondition::Generation(old_head.generation);
    recreated.recreate_after_erasure = true;
    recreated.authorized = repository
        .authorization_snapshot("erase-a", &accepted.source_id)
        .await
        .unwrap();
    let fresh = repository
        .accept(recreated)
        .await
        .expect("explicit fresh incarnation");
    assert_ne!(fresh.source_id, accepted.source_id);
    let intent = client
        .erasure_journal()
        .unwrap()
        .intents()
        .unwrap()
        .into_iter()
        .find(|intent| intent.source_id == accepted.source_id)
        .unwrap();
    repository
        .replay_erasure("erase-a", &accepted.source_id, &intent.intent_id)
        .await
        .unwrap();
    assert!(SourceRepository::new(client.clone())
        .get("erase-a", &fresh.source_id)
        .await
        .unwrap()
        .is_some());
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
async fn fault_boundaries_recover(embedded: bool) {
    for fault in [
        ErasureFault::AfterReservation,
        ErasureFault::AfterJournal,
        ErasureFault::AfterAdmissionReservation,
        ErasureFault::AfterReferenceMembership,
        ErasureFault::DuringPurge,
        ErasureFault::AfterPurge,
        ErasureFault::AfterPersistence,
    ] {
        let client = client(embedded).await;
        let repository = SourcePublicationRepository::new(client.clone());
        let accepted = repository
            .accept(request("erase-a", &format!("fault-{fault:?}")))
            .await
            .unwrap();
        let mutation = mutation(&repository, &accepted, &format!("erase-{fault:?}")).await;
        let operation_id = operation_key("erase-a", &mutation.idempotency_key);
        assert!(repository
            .erase_live_with_fault(mutation, fault)
            .await
            .is_err());
        assert!(!client.recovery_status().ready);
        repository
            .recover_pending_erasures()
            .await
            .expect("recover through real exclusive barrier");
        assert!(client.recovery_status().ready);
        assert!(SourceRepository::new(client.clone())
            .get_with_deleted("erase-a", &accepted.source_id)
            .await
            .unwrap()
            .is_none());
        assert_eq!(
            repository
                .operation("erase-a", &operation_id)
                .await
                .unwrap()
                .unwrap()
                .state,
            OperationState::Erased
        );
    }
}

#[expect(
    clippy::large_futures,
    reason = "Preserve the original future construction and polling shape exercised by this regression."
)]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
async fn retraction_is_reversible_and_keeps_payload(embedded: bool) {
    let client = client(embedded).await;
    let repository = SourcePublicationRepository::new(client.clone());
    let original = request("erase-a", "reversible");
    let accepted = repository.accept(original.clone()).await.unwrap();
    let mutation = mutation(&repository, &accepted, "retract").await;
    let retracted = repository.retract(mutation.clone()).await.unwrap();
    assert_eq!(retracted.state, OperationState::Retracted);
    assert_eq!(repository.retract(mutation).await.unwrap(), retracted);
    assert!(repository
        .revision("erase-a", &accepted.source_id, "r1")
        .await
        .unwrap()
        .is_some());
    assert!(client
        .erasure_journal()
        .unwrap()
        .intents()
        .unwrap()
        .is_empty());
    let mut revived = original;
    revived.revision_id = "r2".into();
    revived.expected = SourcePrecondition::Generation(retracted.source_generation);
    revived.idempotency_key = "explicit-revival".into();
    revived.request_fingerprint = "revival-content".into();
    revived.source.base.deleted_at = None;
    revived.authorized = repository
        .authorization_snapshot("erase-a", &accepted.source_id)
        .await
        .unwrap();
    revived.reactivate = true;
    let revived = repository
        .accept(revived)
        .await
        .expect("guarded explicit restore");
    assert_eq!(revived.source_id, accepted.source_id);
    assert_eq!(
        repository
            .head("erase-a", &accepted.source_id)
            .await
            .unwrap()
            .unwrap()
            .lifecycle,
        SourceVisibility::Active
    );
}

macro_rules! both_backends {
    ($embedded:ident,$surreal:ident,$scenario:ident) => {
        #[tokio::test]
        async fn $embedded() {
            $scenario(true).await;
        }
        #[tokio::test]
        async fn $surreal() {
            $scenario(false).await;
        }
    };
}
both_backends!(
    embedded_stale_authorization_and_cas,
    surreal_stale_authorization_and_cas,
    stale_and_cross_space_refuse_without_intent
);
both_backends!(
    embedded_full_erasure_and_new_incarnation,
    surreal_full_erasure_and_new_incarnation,
    erasure_closure_and_fresh_incarnation
);
both_backends!(
    embedded_durable_fault_boundaries,
    surreal_durable_fault_boundaries,
    fault_boundaries_recover
);
both_backends!(
    embedded_guarded_retraction_restore,
    surreal_guarded_retraction_restore,
    retraction_is_reversible_and_keeps_payload
);

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
async fn shared_support_scrub_keeps_baseline(embedded: bool) {
    let client = client(embedded).await;
    let repository = SourcePublicationRepository::new(client.clone());
    let accepted = repository
        .accept(request("erase-a", "shared-source"))
        .await
        .unwrap();
    let mut baseline = Entity::new(
        BaseFields::new(
            RecordId::new_v4(TableName::Entity),
            "erase-a".into(),
            "authoritative".into(),
        ),
        EntityType::new("project"),
        "Independent original name".into(),
    );
    baseline.aliases = vec!["Independent alias".into()];
    baseline
        .base
        .provenance
        .sources
        .push(accepted.source_id.clone());
    EntityRepository::new(client.clone())
        .create(baseline.clone())
        .await
        .unwrap();
    // Explicitly seed an A-format certified support fixture. The erasure request,
    // reservation, journal, purge and completion still execute through public APIs.
    let support = SourceEffectSupport {
        last_commit_id: None,
        space_id: "erase-a".into(),
        source_id: accepted.source_id.clone(),
        source_revision: "r1".into(),
        record_id: baseline.base.id.clone(),
        independent: true,
        record_fingerprint: crate::canonical_commit::source_support_fingerprint(&baseline).unwrap(),
    };
    if let Some(store) = client.embedded_store() {
        store
            .mutate(|tx| crate::canonical_commit::write_existing_support_tx(tx, &support))
            .await
            .unwrap();
    } else {
        let mut bindings = crate::bindings! {};
        let mut sql = String::new();
        crate::canonical_commit::append_existing_support_sql(
            &mut sql,
            &mut bindings,
            "shared_fixture",
            &support,
        )
        .unwrap();
        client.query_bind_checked(&sql, bindings).await.unwrap();
    }
    let mutate = mutation(&repository, &accepted, "erase-shared").await;
    repository.erase_live(mutate).await.unwrap();
    let after = EntityRepository::new(client.clone())
        .get("erase-a", &baseline.base.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(after.display_name, baseline.display_name);
    assert_eq!(after.aliases, baseline.aliases);
    assert!(after.base.provenance.sources.is_empty());
    assert!(
        repository
            .support_for_record("erase-a", &baseline.base.id)
            .await
            .unwrap()
            .is_empty(),
        "the erased source loses its support even when the independent fact survives"
    );
}
both_backends!(
    embedded_shared_support_keeps_baseline,
    surreal_shared_support_keeps_baseline,
    shared_support_scrub_keeps_baseline
);

// Isolate ledger ownership from factual survival. The composition oracle also
// exercises these rows through the real public IndependentMutation writer.
async fn seed_two_source_audit_support(
    client: &StorageClient,
    first: &RecordId,
    second: &RecordId,
) -> RecordId {
    use elephant_types::records::{AuditAction, AuditLog};
    let mut base = BaseFields::new(
        RecordId::new_v4(TableName::AuditLog),
        "erase-a".into(),
        "independent-owner".into(),
    );
    base.provenance.sources = vec![first.clone(), second.clone()];
    let audit = AuditLog {
        base,
        action: AuditAction::Update,
        actor: "independent-owner".into(),
        record: first.clone(),
        before: Some(serde_json::json!({"source_id": first})),
        after: Some(serde_json::json!({"source_id": second})),
        timestamp: Utc::now(),
        reason: None,
    };
    crate::AuditLogRepository::new(client.clone())
        .create(audit.clone())
        .await
        .unwrap();
    for (index, source) in [first, second].into_iter().enumerate() {
        let support = SourceEffectSupport {
            space_id: "erase-a".into(),
            source_id: source.clone(),
            source_revision: "r1".into(),
            record_id: audit.base.id.clone(),
            // An independent flag cannot keep a ledger whose actual row is gone.
            independent: true,
            record_fingerprint: crate::canonical_commit::source_support_fingerprint(&audit)
                .unwrap(),
            last_commit_id: None,
        };
        if let Some(store) = client.embedded_store() {
            store
                .mutate(|tx| crate::canonical_commit::write_existing_support_tx(tx, &support))
                .await
                .unwrap();
        } else {
            let mut bindings = crate::bindings! {};
            let mut sql = String::new();
            crate::canonical_commit::append_existing_support_sql(
                &mut sql,
                &mut bindings,
                &format!("audit_support_fixture_{index}"),
                &support,
            )
            .unwrap();
            client.query_bind_checked(&sql, bindings).await.unwrap();
        }
    }
    audit.base.id
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
async fn physical_audit_deletion_removes_every_source_support(embedded: bool) {
    let client = client(embedded).await;
    let repository = SourcePublicationRepository::new(client.clone());
    let first = repository
        .accept(request("erase-a", "audit-first"))
        .await
        .unwrap();
    let second = repository
        .accept(request("erase-a", "audit-second"))
        .await
        .unwrap();
    let audit = seed_two_source_audit_support(&client, &first.source_id, &second.source_id).await;
    assert_eq!(
        repository
            .support_for_record("erase-a", &audit)
            .await
            .unwrap()
            .len(),
        2
    );
    let survivor_head = repository.head("erase-a", &first.source_id).await.unwrap();
    // Read-only retraction planning must not remove A's support while retaining
    // the audit payload. The physical erase below has a different obligation.
    let retraction = repository
        .prepare_lifecycle_plan("erase-a", &second.source_id, false)
        .await
        .unwrap();
    assert!(retraction.support_deletes.iter().all(|row| {
        row.value
            .get("source_id")
            .and_then(serde_json::Value::as_str)
            != Some(first.source_id.to_string().as_str())
    }));
    repository
        .erase_live(mutation(&repository, &second, "erase-shared-audit").await)
        .await
        .unwrap();
    assert_eq!(
        repository.head("erase-a", &first.source_id).await.unwrap(),
        survivor_head
    );
    assert!(repository
        .support_for_record("erase-a", &audit)
        .await
        .unwrap()
        .is_empty());
    let inventory = repository.inventory("erase-a").await.unwrap();
    assert!(!inventory
        .iter()
        .any(|row| row.record_id.as_ref() == Some(&audit)));
    assert!(!inventory
        .iter()
        .any(|row| row.table == "source_effect_support"
            && row
                .value
                .get("record_id")
                .and_then(serde_json::Value::as_str)
                == Some(audit.to_string().as_str())));
    assert!(client.recovery_status().ready);
}
both_backends!(
    embedded_physical_audit_deletion_removes_every_source_support,
    surreal_physical_audit_deletion_removes_every_source_support,
    physical_audit_deletion_removes_every_source_support
);

// Reproduce the original committed-purge omission using the real reserved
// transaction and journal writer. Only the obsolete support selection differs;
// no membership, history proof, readiness state, or native row is fabricated.
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
async fn commit_original_orphan_audit_transaction(
    exclusive: &StorageClient,
    intent: &ErasureIntent,
    surviving: &RecordId,
    audit: &RecordId,
) -> StorageResult<()> {
    let repository = SourcePublicationRepository::new(exclusive.clone());
    crate::reference_fence::reserve_reference_guard(exclusive, "erase-a", intent).await?;
    let observed = crate::reference_fence::reference_guard_revision(exclusive, "erase-a").await?;
    let head = repository.head("erase-a", &intent.source_id).await?;
    let mut plan = repository
        .prepare_lifecycle_plan("erase-a", &intent.source_id, true)
        .await?;
    repository
        .prepare_lifecycle_support_continuations(
            "erase-a",
            &intent.source_id,
            &intent.intent_id,
            &mut plan,
            true,
        )
        .await?;
    let count = plan.support_deletes.len();
    plan.support_deletes.retain(|row| {
        !(row
            .value
            .get("source_id")
            .and_then(serde_json::Value::as_str)
            == Some(surviving.to_string().as_str())
            && row
                .value
                .get("record_id")
                .and_then(serde_json::Value::as_str)
                == Some(audit.to_string().as_str()))
    });
    assert_eq!(plan.support_deletes.len() + 1, count);
    assert!(plan
        .deletes
        .iter()
        .any(|row| row.record_id.as_ref() == Some(audit)));
    let memberships = repository.persist_plan_reference_membership(intent, &plan)?;
    let audit_token = crate::reference_fence::reference_identity_token(
        exclusive.erasure_journal()?.identity(),
        "erase-a",
        exclusive.erasure_journal()?.reference_key_bytes(),
        &crate::surreal::thing_from_record_id(audit),
    )?;
    assert!(memberships.contains(&audit_token));
    repository
        .commit_physical_purge(
            "erase-a",
            &intent.source_id,
            &intent.intent_id,
            &plan,
            head.as_ref(),
            &memberships,
            &observed,
            ErasureFault::None,
        )
        .await?;
    let inventory = repository.inventory("erase-a").await?;
    assert!(!inventory
        .iter()
        .any(|row| row.record_id.as_ref() == Some(audit)));
    assert!(inventory
        .iter()
        .any(|row| row.table == "source_effect_support"
            && row
                .value
                .get("source_id")
                .and_then(serde_json::Value::as_str)
                == Some(surviving.to_string().as_str())
            && row
                .value
                .get("record_id")
                .and_then(serde_json::Value::as_str)
                == Some(audit.to_string().as_str())));
    Err(injected_fault())
}

#[expect(
    clippy::large_futures,
    reason = "Preserve the original future construction and polling shape exercised by this regression."
)]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
async fn commit_original_orphan_audit_purge(
    client: &StorageClient,
    erased: &OperationReceipt,
    surviving: &RecordId,
    audit: &RecordId,
) {
    let repository = SourcePublicationRepository::new(client.clone());
    repository
        .erase_live_with_fault(
            mutation(&repository, erased, "interrupt-orphan-audit").await,
            ErasureFault::AfterJournal,
        )
        .await
        .unwrap_err();
    let intent = client
        .erasure_journal()
        .unwrap()
        .intents()
        .unwrap()
        .into_iter()
        .find(|intent| intent.source_id == erased.source_id)
        .unwrap();
    let surviving = surviving.clone();
    let audit = audit.clone();
    let failed: StorageResult<()> = client
        .with_erasure_barrier(move |exclusive| async move {
            exclusive
                .with_erasure_reservation(&intent, async {
                    commit_original_orphan_audit_transaction(
                        &exclusive, &intent, &surviving, &audit,
                    )
                    .await
                })
                .await
        })
        .await;
    assert!(failed.is_err());
    assert!(!client.recovery_status().ready);
}

#[expect(
    clippy::large_futures,
    reason = "Preserve the original future construction and polling shape exercised by this regression."
)]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
async fn original_purge_orphan_support_recovers_from_journal(embedded: bool) {
    let client = client(embedded).await;
    let repository = SourcePublicationRepository::new(client.clone());
    let first = repository
        .accept(request("erase-a", "orphan-first"))
        .await
        .unwrap();
    let second = repository
        .accept(request("erase-a", "orphan-second"))
        .await
        .unwrap();
    let audit = seed_two_source_audit_support(&client, &first.source_id, &second.source_id).await;
    let survivor_head = repository.head("erase-a", &first.source_id).await.unwrap();
    commit_original_orphan_audit_purge(&client, &second, &first.source_id, &audit).await;
    repository.recover_pending_erasures().await.unwrap();
    assert!(client.recovery_status().ready);
    assert_eq!(
        repository.head("erase-a", &first.source_id).await.unwrap(),
        survivor_head
    );
    let inventory = repository.inventory("erase-a").await.unwrap();
    assert!(!inventory
        .iter()
        .any(|row| row.record_id.as_ref() == Some(&audit)));
    assert!(!inventory
        .iter()
        .any(|row| row.table == "source_effect_support"
            && row
                .value
                .get("record_id")
                .and_then(serde_json::Value::as_str)
                == Some(audit.to_string().as_str())));
    assert!(repository
        .support_for_record("erase-a", &audit)
        .await
        .unwrap()
        .is_empty());
    // Once recovered, an unchanged replay must preserve the survivor and stay ready.
    repository.recover_pending_erasures().await.unwrap();
    assert!(client.recovery_status().ready);
    assert_eq!(
        repository.head("erase-a", &first.source_id).await.unwrap(),
        survivor_head
    );
}
both_backends!(
    embedded_original_purge_orphan_support_recovers_from_journal,
    surreal_original_purge_orphan_support_recovers_from_journal,
    original_purge_orphan_support_recovers_from_journal
);

#[test]
fn source_scrub_preserves_semantics_and_refuses_unattributed_payload() {
    let source = RecordId::new_v4(TableName::Source);
    let mut entity = Entity::new(
        BaseFields::new(
            RecordId::new_v4(TableName::Entity),
            "erase-a".into(),
            "test".into(),
        ),
        EntityType::new("project"),
        "independent name".into(),
    );
    entity.base.provenance.sources.push(source.clone());
    entity.aliases.push("independent alias".into());
    let mut row = CanonicalRecord::Entity(Box::new(entity.clone()));
    assert!(scrub_source_lineage(&mut row, &BTreeSet::from([source.id()])).unwrap());
    let CanonicalRecord::Entity(after) = row else {
        unreachable!()
    };
    assert_eq!(after.aliases, entity.aliases);
    assert_eq!(after.display_name, entity.display_name);
    entity.summary = Some(source.to_string());
    let mut row = CanonicalRecord::Entity(Box::new(entity.clone()));
    assert!(scrub_source_lineage(&mut row, &BTreeSet::from([source.id()])).is_err());
    // Mere prose containing an ID is not the supported complete-reference form.
    // Keep independent semantic content while removing its explicit provenance.
    entity.summary = Some(format!("independent prose mentioning {source}"));
    let expected_summary = entity.summary.clone();
    let mut row = CanonicalRecord::Entity(Box::new(entity));
    assert!(scrub_source_lineage(&mut row, &BTreeSet::from([source.id()])).unwrap());
    let CanonicalRecord::Entity(after) = row else {
        unreachable!();
    };
    assert_eq!(after.summary, expected_summary);
    assert!(after.base.provenance.sources.is_empty());
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
#[expect(
    clippy::too_many_lines,
    reason = "Keep the complete ordered conformance scenario and its failure assertions together."
)]
async fn erasure_removes_archived_work_and_staged_artifacts(embedded: bool) {
    use elephant_types::records::{
        ArtifactStatus, WorkArtifact, WorkItem, WorkItemStatus, WorkRun, WorkRunMetrics,
    };
    use elephant_types::Link;
    let client = client(embedded).await;
    let repository = SourcePublicationRepository::new(client.clone());
    let accepted = repository
        .accept(request("erase-a", "work-source"))
        .await
        .unwrap();
    let mut base = BaseFields::new(
        RecordId::new_v4(TableName::WorkItem),
        "erase-a".into(),
        "test".into(),
    );
    base.updated_at = Utc::now() - chrono::Duration::days(10);
    let item = WorkItem {
        base,
        kind: "extract_assertions".into(),
        payload: serde_json::json!({"source_id":accepted.source_id,"text":"ERASE-ME-work-payload"}),
        status: WorkItemStatus::Done,
        priority: 0,
        idempotency_key: "work-erasure".into(),
        attempts: 1,
        lease_expires_at: None,
        leased_by: None,
        run_after: None,
        last_error: None,
    };
    crate::WorkItemRepository::new(client.clone())
        .create(item.clone())
        .await
        .unwrap();
    let run = WorkRun {
        base: BaseFields::new(
            RecordId::new_v4(TableName::WorkRun),
            "erase-a".into(),
            "test".into(),
        ),
        work_item: Link::new(item.base.id.clone()),
        attempt: 1,
        started_at: Utc::now() - chrono::Duration::days(10),
        completed_at: Some(Utc::now() - chrono::Duration::days(10)),
        success: true,
        error: Some("ERASE-ME-run".into()),
        metrics: WorkRunMetrics::default(),
        worker_id: "test".into(),
        worker_version: "1".into(),
        model_version: None,
        prompt_id: None,
        config_hash: None,
    };
    crate::WorkRunRepository::new(client.clone())
        .create(run.clone())
        .await
        .unwrap();
    let artifact = WorkArtifact {
        base: BaseFields::new(
            RecordId::new_v4(TableName::WorkArtifact),
            "erase-a".into(),
            "test".into(),
        ),
        work_run: Link::new(run.base.id.clone()),
        artifact_type: "knowledge_candidates".into(),
        data: serde_json::json!({"text":"ERASE-ME-candidate"}),
        status: ArtifactStatus::Committed,
        validation_errors: None,
        validated_at: None,
        committed_at: Some(Utc::now()),
    };
    crate::WorkArtifactRepository::new(client.clone())
        .create(artifact.clone())
        .await
        .unwrap();
    crate::work_retention::archive_terminal_work(
        &client,
        Utc::now() - chrono::Duration::days(1),
        100,
    )
    .await
    .unwrap();
    let before = repository.inventory("erase-a").await.unwrap();
    assert!(
        before.iter().any(|row| row.table == "work_item_archive"),
        "fixture reached actual cold retention"
    );
    repository
        .erase_live(mutation(&repository, &accepted, "erase-work").await)
        .await
        .unwrap();
    let after = repository.inventory("erase-a").await.unwrap();
    for table in [
        "work_item",
        "work_item_archive",
        "work_run",
        "work_run_archive",
        "work_artifact",
        "work_deadletter",
    ] {
        assert!(
            !after.iter().any(|row| row.table == table
                && row.contains(&BTreeSet::from([
                    item.base.id.id(),
                    run.base.id.id(),
                    artifact.base.id.id()
                ]))),
            "private payload survives in {table}"
        );
    }
    assert!(
        crate::WorkArtifactRepository::new(client.clone())
            .create(artifact)
            .await
            .is_err(),
        "late unannotated artifact still refers to permanently erased run"
    );
}
both_backends!(
    embedded_archived_payload_erasure,
    surreal_archived_payload_erasure,
    erasure_removes_archived_work_and_staged_artifacts
);

#[expect(
    clippy::large_futures,
    reason = "Preserve the original future construction and polling shape exercised by this regression."
)]
async fn erasing_decision_input_restores_independent_baseline(embedded: bool) {
    decision_inverse_with_prior_erasure(embedded, false).await;
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
#[expect(
    clippy::too_many_lines,
    reason = "Keep the complete ordered conformance scenario and its failure assertions together."
)]
async fn decision_inverse_with_prior_erasure(embedded: bool, restore_reserved: bool) {
    use crate::canonical_commit::{
        CanonicalCommit, CanonicalEffect, CanonicalMaintenanceContext, CanonicalPlan,
        PublishedSourceFence,
    };
    let client = client(embedded).await;
    let publication = SourcePublicationRepository::new(client.clone());
    let prior_membership = if restore_reserved {
        let first = publication
            .accept(request("erase-a", "previously-erased-before-reservation"))
            .await
            .unwrap();
        publication
            .erase_live(mutation(&publication, &first, "erase-prior-to-reservation").await)
            .await
            .unwrap();
        let journal = client.erasure_journal().unwrap();
        let intent = journal
            .intents()
            .unwrap()
            .into_iter()
            .find(|intent| intent.source_id == first.source_id)
            .unwrap();
        journal.reference_membership(&intent).unwrap()
    } else {
        BTreeSet::new()
    };
    let original = request("erase-a", "decision-input");
    // Retained maintenance supports sources that predate explicit source heads.
    // Exercise its actual canonical decision boundary before accepting the source.
    let legacy = SourceRepository::new(client.clone())
        .create(original.source.clone())
        .await
        .unwrap();
    let baseline = Entity::new(
        BaseFields::new(
            RecordId::new_v4(TableName::Entity),
            "erase-a".into(),
            "authoritative".into(),
        ),
        EntityType::new("project"),
        "independently supported baseline".into(),
    );
    EntityRepository::new(client.clone())
        .create(baseline.clone())
        .await
        .unwrap();
    let mut hidden = baseline.clone();
    hidden.base.deleted_at = Some(Utc::now());
    let canonical = CanonicalRepository::new(client.clone());
    canonical
        .commit(CanonicalCommit {
            space_id: "erase-a".into(),
            commit_id: "decision-from-b".into(),
            input_fingerprint: "b-supersedes-a".into(),
            replay_output: Some(serde_json::json!({"text":"ERASE-ME-decision-payload"})),
            publication: None,
            finalization: None,
            maintenance: Some(CanonicalMaintenanceContext {
                origin: crate::canonical_commit::CanonicalMaintenanceOrigin::Derived,
                attempt: None,
                sources: vec![PublishedSourceFence::Legacy {
                    source: Box::new(legacy),
                }],
            }),
            plan: CanonicalPlan {
                observed_epoch: canonical.epoch("erase-a").await.unwrap(),
                dependencies: vec![CanonicalRecord::Source(Box::new(original.source.clone()))],
                effects: vec![CanonicalEffect {
                    before: Some(CanonicalRecord::Entity(Box::new(baseline.clone()))),
                    after: Some(CanonicalRecord::Entity(Box::new(hidden))),
                }],
                outbox: vec![],
                extension_effects: vec![],
            },
        })
        .await
        .unwrap();
    assert!(EntityRepository::new(client.clone())
        .get("erase-a", &baseline.base.id)
        .await
        .unwrap()
        .is_none());
    let mut accepted_request = original;
    accepted_request.authorized = publication
        .authorization_snapshot("erase-a", &accepted_request.source.base.id)
        .await
        .unwrap();
    let accepted = publication.accept(accepted_request).await.unwrap();
    let erase = mutation(&publication, &accepted, "erase-decision-input").await;
    if restore_reserved {
        publication
            .erase_live_with_fault(erase, ErasureFault::AfterAdmissionReservation)
            .await
            .unwrap_err();
        assert!(!client.recovery_status().ready);
        let guard = client
            .begin_maintenance(
                crate::recovery_barrier::RecoveryPhase::Replaying,
                std::time::Duration::from_secs(30),
            )
            .await
            .unwrap();
        let privileged = client.maintenance_client_with_guard(&guard);
        let staging = tempfile::tempdir().unwrap();
        // Actual ordinary snapshot selection retains A's anchor count and B's
        // reservation, while excluding every private membership projection.
        let snapshot = privileged
            .snapshot_bytes(&staging.path().join("reserved-backup"))
            .await
            .unwrap();
        assert!(!prior_membership.is_empty());
        for token in &prior_membership {
            assert!(!snapshot
                .windows(token.len())
                .any(|window| window == token.as_bytes()));
        }
        privileged.replace_snapshot_bytes(&snapshot).await.unwrap();
        let replay = SourcePublicationRepository::new(privileged.clone());
        replay.recover_pending_erasures().await.unwrap();
        assert!(crate::reference_fence::reference_reservations(&privileged)
            .await
            .unwrap()
            .is_empty());
        privileged.persist_restored_state().await.unwrap();
        drop(replay);
        drop(privileged);
        guard.finish();
        assert!(client.recovery_status().ready);
    } else {
        publication.erase_live(erase).await.unwrap();
    }
    let restored = EntityRepository::new(client.clone())
        .get("erase-a", &baseline.base.id)
        .await
        .unwrap()
        .expect("baseline restored by real erase transaction");
    assert_eq!(restored.display_name, baseline.display_name);
    assert_eq!(restored.base.deleted_at, None);
    let inventory = publication.inventory("erase-a").await.unwrap();
    assert!(!inventory
        .iter()
        .any(|row| row.table == "source_decision_receipt" || row.table == "source_commit_receipt"));
}
both_backends!(
    embedded_erase_reopens_independent_baseline,
    surreal_erase_reopens_independent_baseline,
    erasing_decision_input_restores_independent_baseline
);

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
async fn old_named_snapshot_replays_real_erasure_before_ready(embedded: bool) {
    let client = client(embedded).await;
    let directory = tempfile::tempdir().unwrap();
    let backup = directory.path().join("before-erasure");
    let publication = SourcePublicationRepository::new(client.clone());
    let accepted = publication
        .accept(request("erase-a", "snapshot-source"))
        .await
        .unwrap();
    let baseline = Entity::new(
        BaseFields::new(
            RecordId::new_v4(TableName::Entity),
            "erase-a".into(),
            "authoritative".into(),
        ),
        EntityType::new("project"),
        "snapshot independent baseline".into(),
    );
    EntityRepository::new(client.clone())
        .create(baseline.clone())
        .await
        .unwrap();
    crate::snapshot::export_named(
        &client,
        &backup,
        "before-source-erasure",
        &crate::snapshot::SnapshotRetention::default(),
    )
    .await
    .unwrap();
    let erased = publication
        .erase_live(mutation(&publication, &accepted, "erase-before-restore").await)
        .await
        .unwrap();
    assert_eq!(
        publication
            .operation("erase-a", &erased.operation_id)
            .await
            .unwrap()
            .unwrap()
            .state,
        OperationState::Erased
    );
    crate::snapshot::restore_named(&client, &backup, client.erasure_journal().unwrap())
        .await
        .expect("journal-aware old snapshot restore");
    assert!(client.recovery_status().ready);
    // An older backup may lose later ordinary operation history. Journal replay
    // preserves physical erasure without inventing a replacement API receipt or
    // exposing the restored acceptance as current/recoverable publication work.
    assert!(publication
        .operation("erase-a", &erased.operation_id)
        .await
        .unwrap()
        .is_none());
    let old_acceptance = publication
        .operation("erase-a", &accepted.operation_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(old_acceptance.state, OperationState::Superseded);
    assert!(old_acceptance.desired_revision.is_none());
    assert!(old_acceptance.published_revision.is_none());
    assert!(!publication
        .recoverable_operations(100)
        .await
        .unwrap()
        .iter()
        .any(|operation| operation.source_id == accepted.source_id));
    assert!(SourceRepository::new(client.clone())
        .get_with_deleted("erase-a", &accepted.source_id)
        .await
        .unwrap()
        .is_none());
    assert!(publication
        .revision("erase-a", &accepted.source_id, "r1")
        .await
        .unwrap()
        .is_none());
    assert_eq!(
        publication
            .head("erase-a", &accepted.source_id)
            .await
            .unwrap()
            .unwrap()
            .lifecycle,
        SourceVisibility::Erased
    );
    assert!(EntityRepository::new(client.clone())
        .get("erase-a", &baseline.base.id)
        .await
        .unwrap()
        .is_some());
}
both_backends!(
    embedded_old_snapshot_replays_real_erasure,
    surreal_old_snapshot_replays_real_erasure,
    old_named_snapshot_replays_real_erasure_before_ready
);

#[expect(
    clippy::panic,
    reason = "Unexpected native values are explicit failures of this regression oracle."
)]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
async fn source_scoped_managed_files_and_vectors_are_physically_removed(embedded: bool) {
    use elephant_types::records::{DocMediaKind, MediaAsset};
    let client = client(embedded).await;
    let publication = SourcePublicationRepository::new(client.clone());
    let mut original = request("erase-a", "asset-source");
    let asset_id = RecordId::new_v4(TableName::MediaAsset);
    let asset = MediaAsset {
        base: BaseFields::new(asset_id.clone(), "erase-a".into(), "test".into()),
        kind: DocMediaKind::Image,
        media_type: "image/png".into(),
        storage_path: elephant_types::records::media_asset::managed_asset_relative_path(&asset_id),
        size_bytes: 12,
        sha256: "opaque-hash".into(),
        payload_base64: Some("RVJBU0UtTUU=".into()),
        duration_ms: None,
        label: Some("private-media".into()),
    };
    original.source.metadata["_manual_doc"]["_intake_assets"] = serde_json::json!([asset.clone()]);
    let accepted = publication.accept(original).await.unwrap();
    crate::MediaAssetRepository::new(client.clone())
        .create(asset.clone())
        .await
        .unwrap();
    let path = elephant_types::records::media_asset::managed_asset_path(&asset_id);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(&path, b"ERASE-ME-file").unwrap();
    let vector_id = Uuid::new_v4();
    let profile_id = Uuid::new_v4();
    let vector = serde_json::json!({"id":format!("emb__erasure_fixture:{vector_id}"),"space_id":"erase-a","node":accepted.source_id.to_string(),"vector":[0.1,0.2],"private":"ERASE-ME-vector"});
    let profile = serde_json::json!({"id":format!("entity_profile_embedding:{profile_id}"),"space_id":"erase-a","entity_id":RecordId::new_v4(TableName::Entity).to_string(),"source_refs":[accepted.source_id.to_string()],"vector":[0.1,0.2]});
    if let Some(store) = client.embedded_store() {
        store
            .mutate(|tx| {
                tx.upsert_extension_row("emb__erasure_fixture", vector.clone())?;
                tx.upsert_extension_row("entity_profile_embedding", profile.clone())?;
                Ok(())
            })
            .await
            .unwrap();
    } else {
        let mut native_vector = crate::surreal::json_to_surreal(vector).unwrap();
        let mut native_profile = crate::surreal::json_to_surreal(profile).unwrap();
        for row in [&mut native_vector, &mut native_profile] {
            let Value::Object(object) = row else {
                panic!("vector fixture must be a native object")
            };
            object.remove("id");
        }
        client
            .query_bind_checked(
                "CREATE $vector_id CONTENT $vector; CREATE $profile_id CONTENT $profile;",
                crate::bindings! {
                    "vector_id" => control_thing("emb__erasure_fixture", &vector_id.to_string()),
                    "vector" => native_vector,
                    "profile_id" => control_thing("entity_profile_embedding", &profile_id.to_string()),
                    "profile" => native_profile,
                },
            )
            .await
            .unwrap();
    }
    publication
        .erase_live(mutation(&publication, &accepted, "erase-files-vectors").await)
        .await
        .unwrap();
    assert!(
        !path.exists(),
        "managed payload must be physically removed before completion"
    );
    assert!(crate::MediaAssetRepository::new(client.clone())
        .get("erase-a", &asset_id)
        .await
        .unwrap()
        .is_none());
    let rows = publication.inventory("erase-a").await.unwrap();
    assert!(!rows.iter().any(|row| row.table == "emb__erasure_fixture"
        || row.table == "entity_profile_embedding"
        || row.table == "source_erasure_files"));
}
both_backends!(
    embedded_managed_files_and_vectors_erased,
    surreal_managed_files_and_vectors_erased,
    source_scoped_managed_files_and_vectors_are_physically_removed
);

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
async fn concurrent_identical_retraction_has_one_receipt(embedded: bool) {
    let client = client(embedded).await;
    let publication = SourcePublicationRepository::new(client.clone());
    let accepted = publication
        .accept(request("erase-a", "same-retract"))
        .await
        .unwrap();
    let request = mutation(&publication, &accepted, "identical-retract").await;
    let (left, right) = tokio::join!(
        publication.retract(request.clone()),
        publication.retract(request)
    );
    assert_eq!(left.unwrap(), right.unwrap());
    assert!(client
        .erasure_journal()
        .unwrap()
        .intents()
        .unwrap()
        .is_empty());
}
both_backends!(
    embedded_concurrent_retract_replays,
    surreal_concurrent_retract_replays,
    concurrent_identical_retraction_has_one_receipt
);

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
#[expect(
    clippy::too_many_lines,
    reason = "Keep the complete ordered conformance scenario and its failure assertions together."
)]
async fn replacement_prefix_retires_support_without_publishing(embedded: bool) {
    use elephant_types::records::{ArtifactStatus, WorkArtifact};
    let client = client(embedded).await;
    let repository = SourcePublicationRepository::new(client.clone());
    let accepted = repository
        .accept(request("erase-a", "replacement-prefix"))
        .await
        .unwrap();
    let mut derived = Entity::new(
        BaseFields::new(
            RecordId::new_v4(TableName::Entity),
            "erase-a".into(),
            "extractor".into(),
        ),
        EntityType::new("project"),
        "only supported by the old source".into(),
    );
    derived
        .base
        .provenance
        .sources
        .push(accepted.source_id.clone());
    EntityRepository::new(client.clone())
        .create(derived.clone())
        .await
        .unwrap();
    let support = SourceEffectSupport {
        last_commit_id: None,
        space_id: "erase-a".into(),
        source_id: accepted.source_id.clone(),
        source_revision: "r0".into(),
        record_id: derived.base.id.clone(),
        independent: false,
        record_fingerprint: crate::canonical_commit::source_support_fingerprint(&derived).unwrap(),
    };
    // Finalization updates an already-existing validated artifact. Its support
    // therefore has an independent baseline, while its immutable publication
    // fence remains source-owned operational data, not a shared semantic fact.
    let mut artifact_base = BaseFields::new(
        RecordId::new_v4(TableName::WorkArtifact),
        "erase-a".into(),
        "extractor".into(),
    );
    artifact_base
        .provenance
        .sources
        .push(accepted.source_id.clone());
    let artifact = WorkArtifact {
        base: artifact_base,
        work_run: elephant_types::Link::new(RecordId::new_v4(TableName::WorkRun)),
        artifact_type: "source_metadata".into(),
        data: serde_json::json!({
            "publication": {"source_id": accepted.source_id, "revision_id": "r0"},
            "text": "ERASE-ME-old-source-metadata"
        }),
        status: ArtifactStatus::Committed,
        validation_errors: None,
        validated_at: Some(Utc::now()),
        committed_at: Some(Utc::now()),
    };
    let artifacts = crate::WorkArtifactRepository::new(client.clone());
    artifacts.create(artifact.clone()).await.unwrap();
    let artifact_support = SourceEffectSupport {
        space_id: "erase-a".into(),
        source_id: accepted.source_id.clone(),
        source_revision: "r0".into(),
        record_id: artifact.base.id.clone(),
        independent: true,
        last_commit_id: None,
        record_fingerprint: crate::canonical_commit::source_support_fingerprint(&artifact).unwrap(),
    };
    for (index, support) in [support, artifact_support].iter().enumerate() {
        if let Some(store) = client.embedded_store() {
            store
                .mutate(|tx| crate::canonical_commit::write_existing_support_tx(tx, support))
                .await
                .unwrap();
        } else {
            let mut bindings = crate::bindings! {};
            let mut sql = String::new();
            crate::canonical_commit::append_existing_support_sql(
                &mut sql,
                &mut bindings,
                &format!("prefix_fixture_{index}"),
                support,
            )
            .unwrap();
            client.query_bind_checked(&sql, bindings).await.unwrap();
        }
    }
    let head = repository
        .head("erase-a", &accepted.source_id)
        .await
        .unwrap()
        .unwrap();
    let fence = PublicationFence {
        source_id: accepted.source_id.clone(),
        generation: head.generation.clone(),
        revision_id: head.desired_revision.clone().unwrap(),
        operation_id: head.operation_id.clone(),
    };
    let prepared = repository
        .prepare_source_replacement("erase-a", &fence)
        .await
        .unwrap();
    let retirement = prepared
        .plan
        .effects
        .iter()
        .find(|effect| {
            effect
                .before
                .as_ref()
                .is_some_and(|row| row.base().id == derived.base.id)
        })
        .unwrap();
    assert!(retirement
        .after
        .as_ref()
        .unwrap()
        .base()
        .deleted_at
        .is_some());
    assert_eq!(prepared.control_deletes.len(), 2);
    assert!(prepared.control_deletes.iter().all(|(table, key)| {
        table == "source_effect_support" && key.starts_with("source_effect_support:")
    }));
    assert!(!prepared.plan.effects.iter().any(|effect| {
        effect
            .after
            .as_ref()
            .or(effect.before.as_ref())
            .is_some_and(|row| row.base().id == artifact.base.id)
    }));
    assert_eq!(
        artifacts
            .get_including_deleted("erase-a", &artifact.base.id)
            .await
            .unwrap(),
        Some(artifact.clone()),
        "replacement planning leaves the original artifact and its source fence intact"
    );
    assert!(prepared.plan.outbox.is_empty());
    assert!(!prepared.plan.effects.iter().any(|effect| {
        effect
            .after
            .as_ref()
            .or(effect.before.as_ref())
            .is_some_and(|row| row.base().id == accepted.source_id)
    }));
    assert_eq!(
        repository
            .head("erase-a", &accepted.source_id)
            .await
            .unwrap()
            .unwrap(),
        head
    );
    assert!(EntityRepository::new(client.clone())
        .get("erase-a", &derived.base.id)
        .await
        .unwrap()
        .is_some());
    assert!(SourceRepository::new(client.clone())
        .get("erase-a", &accepted.source_id)
        .await
        .unwrap()
        .is_some());
    let mut stale = fence;
    stale.generation = "stale-generation".into();
    assert!(repository
        .prepare_source_replacement("erase-a", &stale)
        .await
        .is_err());
    repository
        .erase_live(mutation(&repository, &accepted, "erase-operational-support").await)
        .await
        .unwrap();
    assert!(
        artifacts
            .get_including_deleted("erase-a", &artifact.base.id)
            .await
            .unwrap()
            .is_none(),
        "physical erasure purges source-owned operational payload despite independent support"
    );
}
both_backends!(
    embedded_source_replacement_prefix_is_read_only,
    surreal_source_replacement_prefix_is_read_only,
    replacement_prefix_retires_support_without_publishing
);

#[expect(
    clippy::large_futures,
    reason = "Preserve the original future construction and polling shape exercised by this regression."
)]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
async fn document_intent_checks_original_renderer_snapshot(embedded: bool) {
    use elephant_types::records::Doc;
    let client = client(embedded).await;
    let repository = SourcePublicationRepository::new(client.clone());
    let accepted = repository
        .accept(request("erase-a", "doc-origin"))
        .await
        .unwrap();
    let documents = crate::DocRepository::new(client.clone());
    let original = Doc {
        base: BaseFields::new(
            RecordId::new_v4(TableName::Doc),
            "erase-a".into(),
            "renderer".into(),
        ),
        title: "Original renderer output".into(),
        text: "authorized exact document".into(),
        sidecar_text: None,
        source: Some(elephant_types::Link::new(accepted.source_id.clone())),
        media_attachments: vec![],
        rendition_key: "doc-origin-r1".into(),
        metadata: serde_json::json!({}),
    };
    documents.create(original.clone()).await.unwrap();
    let mut erase = mutation(&repository, &accepted, "erase-doc-origin").await;
    erase.authorized_document = Some(original.clone());
    let mut retract = mutation(&repository, &accepted, "retract-doc-origin").await;
    retract.authorized_document = Some(original.clone());
    let mut replaced = original.clone();
    replaced.text = "concurrent renderer output".into();
    documents
        .update("erase-a", &original.base.id, replaced)
        .await
        .unwrap();
    let before = repository
        .head("erase-a", &accepted.source_id)
        .await
        .unwrap();
    assert!(repository.erase_live(erase).await.is_err());
    assert!(repository.retract(retract).await.is_err());
    assert_eq!(
        repository
            .head("erase-a", &accepted.source_id)
            .await
            .unwrap(),
        before
    );
    assert!(client
        .erasure_journal()
        .unwrap()
        .intents()
        .unwrap()
        .is_empty());
    let mut valid = mutation(&repository, &accepted, "erase-current-doc-origin").await;
    valid.authorized_document = documents.get("erase-a", &original.base.id).await.unwrap();
    assert_eq!(
        repository.erase_live(valid).await.unwrap().state,
        OperationState::Erased
    );
    assert!(documents
        .get("erase-a", &original.base.id)
        .await
        .unwrap()
        .is_none());
}
both_backends!(
    embedded_document_origin_cannot_erase_changed_renderer,
    surreal_document_origin_cannot_erase_changed_renderer,
    document_intent_checks_original_renderer_snapshot
);

#[expect(
    clippy::panic,
    reason = "Unexpected native values are explicit failures of this regression oracle."
)]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
#[expect(
    clippy::too_many_lines,
    reason = "Keep the complete ordered conformance scenario and its failure assertions together."
)]
async fn erase_reverses_declared_extension_decision(embedded: bool) {
    use crate::canonical_commit::{
        CanonicalCommit, CanonicalMaintenanceContext, CanonicalMaintenanceOrigin, CanonicalPlan,
        PublishedSourceFence,
    };
    use crate::canonical_extension::{load_extension_record, CanonicalExtensionEffect};
    let client = client(embedded).await;
    let publication = SourcePublicationRepository::new(client.clone());
    let original = request("erase-a", "extension-decision-input");
    let legacy = SourceRepository::new(client.clone())
        .create(original.source.clone())
        .await
        .unwrap();
    let extension_id = Thing::from(("consumer_extension".to_string(), Uuid::new_v4().to_string()));
    let row = serde_json::json!({"id":extension_id.to_string(),"space_id":"erase-a",
        "nested":{"label":"independent-original","preserve":"baseline"}});
    if let Some(store) = client.embedded_store() {
        store
            .mutate(|tx| {
                tx.upsert_extension_row("consumer_extension", row.clone())
                    .map_err(Into::into)
            })
            .await
            .unwrap();
    } else {
        let mut native = crate::surreal::json_to_surreal(row).unwrap();
        if let Value::Object(object) = &mut native {
            object.insert("id".into(), Value::Thing(extension_id.clone()));
        }
        client
            .query_bind_checked(
                "CREATE $id CONTENT $row",
                crate::bindings! {
                    "id" => extension_id.clone(), "row" => native,
                },
            )
            .await
            .unwrap();
    }
    let before = load_extension_record(&client, "erase-a", &extension_id)
        .await
        .unwrap()
        .unwrap();
    let mut after = before.clone();
    let Value::Object(object) = &mut after else {
        panic!("extension object")
    };
    let Some(Value::Object(nested)) = object.get_mut("nested") else {
        panic!("nested object")
    };
    nested.insert("label".into(), Value::from("ERASE-ME-derived-choice"));
    let canonical = CanonicalRepository::new(client.clone());
    canonical
        .commit(CanonicalCommit {
            space_id: "erase-a".into(),
            commit_id: "extension-derived-decision".into(),
            input_fingerprint: "extension-input".into(),
            replay_output: None,
            publication: None,
            finalization: None,
            maintenance: Some(CanonicalMaintenanceContext {
                origin: CanonicalMaintenanceOrigin::Derived,
                attempt: None,
                sources: vec![PublishedSourceFence::Legacy {
                    source: Box::new(legacy),
                }],
            }),
            plan: CanonicalPlan {
                observed_epoch: canonical.epoch("erase-a").await.unwrap(),
                dependencies: vec![CanonicalRecord::Source(Box::new(original.source.clone()))],
                effects: vec![],
                outbox: vec![],
                extension_effects: vec![CanonicalExtensionEffect {
                    id: extension_id.clone(),
                    before: Some(before.clone()),
                    after: Some(after),
                }],
            },
        })
        .await
        .unwrap();
    let mut acceptance = original;
    acceptance.authorized = publication
        .authorization_snapshot("erase-a", &acceptance.source.base.id)
        .await
        .unwrap();
    let accepted = publication.accept(acceptance).await.unwrap();
    publication
        .erase_live(mutation(&publication, &accepted, "erase-extension-source").await)
        .await
        .unwrap();
    assert_eq!(
        load_extension_record(&client, "erase-a", &extension_id)
            .await
            .unwrap(),
        Some(before)
    );
    assert!(publication
        .inventory("erase-a")
        .await
        .unwrap()
        .iter()
        .all(|row| !row.value.to_string().contains("ERASE-ME-derived-choice")));
}
both_backends!(
    embedded_erasure_reverses_native_extension_decision,
    surreal_erasure_reverses_native_extension_decision,
    erase_reverses_declared_extension_decision
);

#[test]
fn native_inventory_is_only_a_lossless_key_and_reference_search_projection() {
    let source = RecordId::new_v4(TableName::Source);
    let mut fields = BTreeMap::<String, Value>::new();
    fields.insert("type".into(), Value::from("custom-extension-kind"));
    fields.insert(
        "nullable_object".into(),
        Value::Object(surrealdb::sql::Object::from(
            BTreeMap::<String, Value>::from([("source_hint".into(), Value::None)]),
        )),
    );
    fields.insert(
        "lineage".into(),
        Value::Array(Array::from(vec![Value::Thing(
            crate::surreal::thing_from_record_id(&source),
        )])),
    );
    let projected = inventory_json(Value::Object(surrealdb::sql::Object::from(fields)));
    assert_eq!(projected["type"], "custom-extension-kind");
    assert!(projected["nullable_object"].is_object());
    assert!(contains_reference(
        &projected,
        &BTreeSet::from([source.id()])
    ));
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
async fn inverse_deleted_output_seeds_erasure_closure(embedded: bool) {
    use crate::canonical_commit::{
        CanonicalCommit, CanonicalMaintenanceContext, CanonicalMaintenanceOrigin, CanonicalPlan,
        PublishedSourceFence,
    };
    use elephant_types::records::{ArtifactStatus, WorkArtifact};
    let client = client(embedded).await;
    let publication = SourcePublicationRepository::new(client.clone());
    let original = request("erase-a", "inverse-only-link");
    let legacy = SourceRepository::new(client.clone())
        .create(original.source.clone())
        .await
        .unwrap();
    let output = Entity::new(
        BaseFields::new(
            RecordId::new_v4(TableName::Entity),
            "erase-a".into(),
            "decision".into(),
        ),
        EntityType::new("project"),
        "ERASE-ME-exclusive-derived-output".into(),
    );
    let canonical = CanonicalRepository::new(client.clone());
    canonical
        .commit(CanonicalCommit {
            space_id: "erase-a".into(),
            commit_id: "derived-output-only".into(),
            input_fingerprint: "derived-output".into(),
            replay_output: None,
            publication: None,
            finalization: None,
            maintenance: Some(CanonicalMaintenanceContext {
                origin: CanonicalMaintenanceOrigin::Derived,
                attempt: None,
                sources: vec![PublishedSourceFence::Legacy {
                    source: Box::new(legacy),
                }],
            }),
            plan: CanonicalPlan {
                observed_epoch: canonical.epoch("erase-a").await.unwrap(),
                dependencies: vec![CanonicalRecord::Source(Box::new(original.source.clone()))],
                effects: vec![CanonicalEffect {
                    before: None,
                    after: Some(CanonicalRecord::Entity(Box::new(output.clone()))),
                }],
                extension_effects: vec![],
                outbox: vec![],
            },
        })
        .await
        .unwrap();
    // This raw artifact only names the derived output. It has no source or
    // supporting revision reference that could independently seed cleanup.
    let artifact = WorkArtifact {
        base: BaseFields::new(
            RecordId::new_v4(TableName::WorkArtifact),
            "erase-a".into(),
            "test".into(),
        ),
        work_run: elephant_types::Link::new(RecordId::new_v4(TableName::WorkRun)),
        artifact_type: "derived_choice".into(),
        data: serde_json::json!({"output":output.base.id,"text":"ERASE-ME-dependent-artifact"}),
        status: ArtifactStatus::Committed,
        validation_errors: None,
        validated_at: None,
        committed_at: Some(Utc::now()),
    };
    let artifacts = crate::WorkArtifactRepository::new(client.clone());
    artifacts.create(artifact.clone()).await.unwrap();
    let mut acceptance = original;
    acceptance.authorized = publication
        .authorization_snapshot("erase-a", &acceptance.source.base.id)
        .await
        .unwrap();
    let accepted = publication.accept(acceptance).await.unwrap();
    publication
        .erase_live(mutation(&publication, &accepted, "erase-inverse-output").await)
        .await
        .unwrap();
    assert!(EntityRepository::new(client.clone())
        .get("erase-a", &output.base.id)
        .await
        .unwrap()
        .is_none());
    assert!(artifacts
        .get_including_deleted("erase-a", &artifact.base.id)
        .await
        .unwrap()
        .is_none());
    assert!(
        artifacts.create(artifact).await.is_err(),
        "inverse output has a permanent late-writer fence"
    );
}
both_backends!(
    embedded_inverse_deleted_output_has_physical_closure,
    surreal_inverse_deleted_output_has_physical_closure,
    inverse_deleted_output_seeds_erasure_closure
);

#[expect(
    clippy::large_futures,
    reason = "Preserve the original future construction and polling shape exercised by this regression."
)]
async fn nonuuid_extension_references_join_physical_closure(embedded: bool) {
    for key in ["named-row", "Luka's Ω \"quoted"] {
        erase_named_extension_reference(embedded, key).await;
    }
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
async fn erase_named_extension_reference(embedded: bool, key: &str) {
    use elephant_types::records::{ArtifactStatus, WorkArtifact};
    let client = client(embedded).await;
    let publication = SourcePublicationRepository::new(client.clone());
    let accepted = publication
        .accept(request("erase-a", "string-extension-link"))
        .await
        .unwrap();
    let id = Thing::from(("consumer_note", key));
    let row = serde_json::json!({"id":id.to_string(),"space_id":"erase-a",
        "source_id":accepted.source_id,"text":"ERASE-ME-extension"});
    if let Some(store) = client.embedded_store() {
        store
            .mutate(|tx| {
                tx.upsert_extension_row("consumer_note", row.clone())
                    .map_err(Into::into)
            })
            .await
            .unwrap();
    } else {
        let mut value = crate::surreal::json_to_surreal(row).unwrap();
        if let Value::Object(object) = &mut value {
            object.insert("id".into(), Value::Thing(id.clone()));
        }
        client
            .query_bind_checked(
                "CREATE $id CONTENT $row",
                crate::bindings! {"id"=>id.clone(),"row"=>value},
            )
            .await
            .unwrap();
    }
    let artifact = WorkArtifact {
        base: BaseFields::new(
            RecordId::new_v4(TableName::WorkArtifact),
            "erase-a".into(),
            "test".into(),
        ),
        work_run: elephant_types::Link::new(RecordId::new_v4(TableName::WorkRun)),
        artifact_type: "extension_decision".into(),
        data: serde_json::json!({"input":id.to_string(),"text":"ERASE-ME-named-ref"}),
        status: ArtifactStatus::Committed,
        validation_errors: None,
        validated_at: None,
        committed_at: Some(Utc::now()),
    };
    let artifacts = crate::WorkArtifactRepository::new(client.clone());
    artifacts.create(artifact.clone()).await.unwrap();
    let mut related_ids = Vec::new();
    for data in [
        serde_json::json!({"nested":serde_json::to_string(&serde_json::json!({"ref":id.to_string()})).unwrap()}),
        serde_json::json!({"lookup":{id.to_string():"ERASE-ME-reference-key"}}),
    ] {
        let mut related = artifact.clone();
        related.base.id = RecordId::new_v4(TableName::WorkArtifact);
        related.data = data;
        artifacts.create(related.clone()).await.unwrap();
        related_ids.push(related.base.id);
    }
    let mut independent_ids = Vec::new();
    for data in [
        serde_json::json!({"input":format!("{}-extra", id)}),
        serde_json::json!({"text":format!("prose mentions {id} inline")}),
    ] {
        let mut independent = artifact.clone();
        independent.base.id = RecordId::new_v4(TableName::WorkArtifact);
        independent.data = data;
        artifacts.create(independent.clone()).await.unwrap();
        independent_ids.push(independent.base.id);
    }
    publication
        .erase_live(mutation(&publication, &accepted, "erase-string-extension").await)
        .await
        .unwrap();
    assert!(artifacts
        .get_including_deleted("erase-a", &artifact.base.id)
        .await
        .unwrap()
        .is_none());
    assert!(publication
        .inventory("erase-a")
        .await
        .unwrap()
        .iter()
        .all(|row| row.key != id.to_string()));
    for id in related_ids {
        assert!(artifacts
            .get_including_deleted("erase-a", &id)
            .await
            .unwrap()
            .is_none());
    }
    for id in independent_ids {
        assert!(artifacts
            .get_including_deleted("erase-a", &id)
            .await
            .unwrap()
            .is_some());
    }
}
both_backends!(
    embedded_nonuuid_extension_dependency_erasure,
    surreal_nonuuid_extension_dependency_erasure,
    nonuuid_extension_references_join_physical_closure
);

#[test]
fn erasure_dependency_keys_are_references_too() {
    let source = RecordId::new_v4(TableName::Source);
    let value = serde_json::json!({"lookup":{source.to_string(): "derived payload"}});
    assert!(contains_reference(&value, &BTreeSet::from([source.id()])));
    let value = serde_json::json!({"lookup":{"consumer_note:named-row":"derived payload"}});
    assert!(contains_key_reference(
        &value,
        &BTreeSet::from(["consumer_note:named-row".into()])
    ));
}

#[test]
fn native_key_dependencies_match_exact_serialized_references() {
    let keys = BTreeSet::from(["consumer_note:named-row".to_string()]);
    for value in [
        serde_json::json!("consumer_note:named-row"),
        serde_json::json!({"reference": "consumer_note:named-row"}),
        serde_json::json!(r#" {"reference":"consumer_note:named-row"}"#),
        serde_json::json!(r#""consumer_note:named-row""#),
    ] {
        assert!(contains_key_reference(&value, &keys), "{value}");
    }
    for value in [
        serde_json::json!("consumer_note:named-row-extra"),
        serde_json::json!("prefix-consumer_note:named-row"),
        serde_json::json!("prose mentions consumer_note:named-row inline"),
        serde_json::json!({"consumer_note:named-row-extra": "unrelated"}),
    ] {
        assert!(!contains_key_reference(&value, &keys), "{value}");
    }
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
async fn native_typed_extension_closure_preserves_independent_identity(embedded: bool) {
    use elephant_types::records::{ArtifactStatus, WorkArtifact};
    use surrealdb::sql::Id;
    let client = client(embedded).await;
    let publication = SourcePublicationRepository::new(client.clone());
    let accepted = publication
        .accept(request("erase-a", "typed-extension-reference"))
        .await
        .unwrap();
    let string_id = Thing::from(("consumer_note", "42"));
    let number_id = Thing::from(("consumer_note".to_string(), Id::Number(42)));
    assert_ne!(string_id, number_id);
    assert_ne!(string_id.to_string(), number_id.to_string());
    for (id, source_owned) in [(&string_id, false), (&number_id, true)] {
        let mut row =
            serde_json::json!({"id":id.to_string(),"space_id":"erase-a","text":"native-identity"});
        if source_owned {
            row["source_id"] = serde_json::json!(accepted.source_id);
        }
        if let Some(store) = client.embedded_store() {
            store
                .mutate(|tx| {
                    tx.upsert_extension_row("consumer_note", row.clone())
                        .map_err(Into::into)
                })
                .await
                .unwrap();
        } else {
            let mut value = crate::surreal::json_to_surreal(row).unwrap();
            if let Value::Object(object) = &mut value {
                object.remove("id");
            }
            client
                .query_bind_checked(
                    "CREATE $id CONTENT $row",
                    crate::bindings! {"id"=>id.clone(),"row"=>value},
                )
                .await
                .unwrap();
        }
    }
    let artifacts = crate::WorkArtifactRepository::new(client.clone());
    let artifact = WorkArtifact {
        base: BaseFields::new(
            RecordId::new_v4(TableName::WorkArtifact),
            "erase-a".into(),
            "test".into(),
        ),
        work_run: elephant_types::Link::new(RecordId::new_v4(TableName::WorkRun)),
        artifact_type: "typed_extension_reference".into(),
        data: serde_json::json!({"reference":{"tb":"consumer_note","id":42}}),
        status: ArtifactStatus::Committed,
        validation_errors: None,
        validated_at: None,
        committed_at: Some(Utc::now()),
    };
    artifacts.create(artifact.clone()).await.unwrap();
    let mut independent = artifact.clone();
    independent.base.id = RecordId::new_v4(TableName::WorkArtifact);
    independent.data = serde_json::json!({"reference":{"tb":"consumer_note","id":"42"}});
    artifacts.create(independent.clone()).await.unwrap();
    publication
        .erase_live(mutation(&publication, &accepted, "erase-numeric-extension").await)
        .await
        .unwrap();
    assert!(artifacts
        .get_including_deleted("erase-a", &artifact.base.id)
        .await
        .unwrap()
        .is_none());
    assert!(artifacts
        .get_including_deleted("erase-a", &independent.base.id)
        .await
        .unwrap()
        .is_some());
    let inventory = publication.inventory("erase-a").await.unwrap();
    assert!(inventory
        .iter()
        .all(|row| row.reference_key() != number_id.to_string()));
    assert!(inventory
        .iter()
        .any(|row| row.reference_key() == string_id.to_string()));
}
both_backends!(
    embedded_erasure_keeps_native_string_number_distinct,
    surreal_erasure_keeps_native_string_number_distinct,
    native_typed_extension_closure_preserves_independent_identity
);

#[test]
fn embedded_extension_physical_key_and_reference_identity_are_separate() {
    for id in [
        serde_json::json!("Luka's Ω \"quoted"),
        serde_json::json!({"tb":"consumer_note","id":42}),
        serde_json::json!({"table":"consumer_note","id":"42"}),
    ] {
        let value = serde_json::json!({"id":id,"space_id":"erase-a"});
        let native =
            crate::canonical_extension::embedded_extension_identity("consumer_note", &value)
                .unwrap();
        let row = InventoryRow {
            table: "consumer_note".into(),
            key: "original-physical-key".into(),
            native_id: Some(native.clone()),
            record_id: None,
            value,
            native_value: None,
        };
        assert_eq!(row.key, "original-physical-key");
        assert_eq!(row.reference_key(), native.to_string());
        assert!(contains_key_reference(
            &serde_json::json!({"ref":native.to_string()}),
            &BTreeSet::from([row.reference_key()])
        ));
    }
}

#[test]
fn opaque_uuid_keys_keep_native_type_and_table() {
    use surrealdb::sql::Id;
    let uuid = Uuid::new_v4();
    let string_id = Thing::from(("consumer_note".to_string(), Id::String(uuid.to_string())));
    let native_id = Thing::from(("consumer_note".to_string(), Id::Uuid(uuid.into())));
    let other_table = Thing::from(("independent_note".to_string(), Id::String(uuid.to_string())));
    let removed = BTreeSet::from([normalized_native_key(&string_id)]);
    assert!(contains_key_reference(
        &serde_json::json!(string_id.to_string()),
        &removed
    ));
    assert!(!contains_key_reference(
        &serde_json::json!(native_id.to_string()),
        &removed
    ));
    assert!(!contains_key_reference(
        &serde_json::json!(other_table.to_string()),
        &removed
    ));
}

#[test]
fn private_uuid_keys_use_typed_identity_without_changing_extension_keys() {
    use surrealdb::sql::Id;
    let uuid = Uuid::parse_str("962f31d9-5b53-5351-b4c6-9342a70ff14d").unwrap();
    for table in ["source_effect_support", "source_commit_receipt", "entity"] {
        let string = Thing::from((table.to_owned(), Id::String(uuid.to_string())));
        let native = Thing::from((table.to_owned(), Id::Uuid(uuid.into())));
        assert_eq!(normalized_native_key(&string), format!("{table}:{uuid}"));
        assert_eq!(normalized_native_key(&native), format!("{table}:{uuid}"));
    }
    let private = control_thing("source_effect_support", &uuid.to_string());
    assert!(
        private.to_string().contains('`'),
        "exercise the original native String UUID display quoting"
    );
    for identity in [
        Thing::from(("consumer_note", Id::String(uuid.to_string()))),
        Thing::from(("consumer_note", Id::Uuid(uuid.into()))),
        Thing::from(("consumer_note", Id::Number(42))),
        control_thing("source_effect_support", "not-a-uuid"),
        control_thing("source_effect_support", &format!("`{uuid}`")),
        Thing::from(("source_effect_support", Id::Number(42))),
    ] {
        assert_eq!(normalized_native_key(&identity), identity.to_string());
    }
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
async fn native_private_string_uuid_inventory_key_deletes_only_original_control() {
    // This is a physical key/deletion seam, not publication authority. Actual
    // source replacement coverage uses the original completed contact fixtures.
    let client = client(false).await;
    let repository = SourcePublicationRepository::new(client.clone());
    let uuid = "962f31d9-5b53-5351-b4c6-9342a70ff14d";
    let original = control_thing("source_effect_support", uuid);
    let literal_quotes = control_thing("source_effect_support", &format!("`{uuid}`"));
    assert_ne!(original, literal_quotes);
    client
        .query_bind_checked(
            "CREATE $original SET space_id=$space, identity_fixture='original'; CREATE $quoted SET space_id=$space, identity_fixture='literal quoted key';",
            crate::bindings! {
                "space" => "erase-a",
                "original" => original.clone(),
                "quoted" => literal_quotes.clone(),
            },
        )
        .await
        .unwrap();
    let inventory = repository.inventory("erase-a").await.unwrap();
    let original_row = inventory
        .iter()
        .find(|row| row.native_id.as_ref() == Some(&original))
        .unwrap();
    assert_eq!(original_row.key, format!("source_effect_support:{uuid}"));
    let bare_key = original_row
        .key
        .strip_prefix("source_effect_support:")
        .unwrap();
    // Exact native reconstruction used by canonical replacement retirement.
    let deletion = control_thing("source_effect_support", bare_key);
    assert_eq!(deletion, original);
    client
        .query_bind_checked(
            "DELETE $retired_control;",
            crate::bindings! { "retired_control" => deletion },
        )
        .await
        .unwrap();
    let after = repository.inventory("erase-a").await.unwrap();
    assert!(!after
        .iter()
        .any(|row| row.native_id.as_ref() == Some(&original)));
    let quoted_row = after
        .iter()
        .find(|row| row.native_id.as_ref() == Some(&literal_quotes))
        .unwrap();
    let before_quoted = inventory
        .iter()
        .find(|row| row.native_id.as_ref() == Some(&literal_quotes))
        .unwrap();
    assert_eq!(quoted_row.native_value, before_quoted.native_value);
}

#[expect(
    clippy::large_futures,
    reason = "Preserve the original future construction and polling shape exercised by this regression."
)]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
async fn lifecycle_events_keep_source_authorization_envelope(embedded: bool) {
    let client = client(embedded).await;
    let repository = SourcePublicationRepository::new(client.clone());
    let sentinel = SystemEvent {
        base: BaseFields::new(
            RecordId::new_v4(TableName::SystemEvent),
            "erase-a".into(),
            "independent".into(),
        ),
        seq: 0,
        event_type: "independent.zero".into(),
        payload: serde_json::json!({"retained":true}),
    };
    if let Some(store) = client.embedded_store() {
        store
            .mutate(|tx| tx.create(sentinel.clone()).map_err(Into::into))
            .await
            .unwrap();
    } else {
        client
            .query_bind_checked(
                "CREATE $id CONTENT $row",
                crate::bindings! {
                    "id"=>crate::surreal::thing_from_record_id(&sentinel.base.id),
                    "row"=>crate::SystemEventRepository::system_event_to_value(&sentinel).unwrap(),
                },
            )
            .await
            .unwrap();
    }
    for erase in [false, true] {
        let mut original = request(
            "erase-a",
            if erase {
                "classified-erase"
            } else {
                "classified-retract"
            },
        );
        original.source.base.security.policy_id = "private-domain".into();
        original.source.base.security.level = 4;
        original.source.base.security.labels = vec!["restricted".into()];
        original.source.base.security.handling = vec!["no_external".into()];
        original.source.base.subjects = vec![elephant_types::Link::new(RecordId::new_v4(
            TableName::Entity,
        ))];
        original.source.base.subjects_state = elephant_types::records::SubjectsState::Partial;
        let expected = SourceAuthorizationEnvelope::from(&original.source);
        let accepted = repository.accept(original).await.unwrap();
        let request = mutation(
            &repository,
            &accepted,
            if erase {
                "erase-classified"
            } else {
                "retract-classified"
            },
        )
        .await;
        let event_type = if erase {
            repository.erase_live(request).await.unwrap();
            "source.erased"
        } else {
            repository.retract(request).await.unwrap();
            "source.retracted"
        };
        let events = crate::SystemEventRepository::new(client.clone())
            .find_recent_by_type_since(event_type, Utc::now() - chrono::Duration::minutes(1), 10)
            .await
            .unwrap();
        let event = events
            .iter()
            .find(|event| event.payload["source_id"] == serde_json::json!(accepted.source_id))
            .unwrap();
        assert_eq!(event.base.security, expected.security);
        assert_eq!(event.base.subjects, expected.subjects);
        assert_eq!(event.base.subjects_state, expected.subjects_state);
        assert!(!event.payload.to_string().contains("ERASE-ME"));
    }
}
both_backends!(
    embedded_source_lifecycle_event_authorization,
    surreal_source_lifecycle_event_authorization,
    lifecycle_events_keep_source_authorization_envelope
);

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
async fn snapshot_predating_source_keeps_journal_incarnation_fence(embedded: bool) {
    use elephant_types::records::{ArtifactStatus, WorkArtifact};
    let client = client(embedded).await;
    let directory = tempfile::tempdir().unwrap();
    let backup = directory.path().join("before-source-existed");
    crate::snapshot::export_named(
        &client,
        &backup,
        "before-source-existed",
        &crate::snapshot::SnapshotRetention::default(),
    )
    .await
    .unwrap();
    let publication = SourcePublicationRepository::new(client.clone());
    let accepted = publication
        .accept(request("erase-a", "future-erased-source"))
        .await
        .unwrap();
    publication
        .erase_live(mutation(&publication, &accepted, "erase-future-source").await)
        .await
        .unwrap();
    crate::snapshot::restore_named(&client, &backup, client.erasure_journal().unwrap())
        .await
        .unwrap();
    assert!(client.recovery_status().ready);
    assert!(publication
        .head("erase-a", &accepted.source_id)
        .await
        .unwrap()
        .is_none());
    assert!(SourceRepository::new(client.clone())
        .get_with_deleted("erase-a", &accepted.source_id)
        .await
        .unwrap()
        .is_none());
    let artifact = WorkArtifact {
        base: BaseFields::new(
            RecordId::new_v4(TableName::WorkArtifact),
            "erase-a".into(),
            "late-producer".into(),
        ),
        work_run: elephant_types::Link::new(RecordId::new_v4(TableName::WorkRun)),
        artifact_type: "delayed_original_source".into(),
        data: serde_json::json!({"source_id":accepted.source_id,"text":"ERASE-ME-after-old-restore"}),
        status: ArtifactStatus::Draft,
        validation_errors: None,
        validated_at: None,
        committed_at: None,
    };
    let artifacts = crate::WorkArtifactRepository::new(client.clone());
    assert!(
        artifacts.create(artifact.clone()).await.is_err(),
        "journal-derived fence survives backup before source creation"
    );
    assert!(artifacts
        .get_including_deleted("erase-a", &artifact.base.id)
        .await
        .unwrap()
        .is_none());
}
both_backends!(
    embedded_pre_source_backup_preserves_erasure_fence,
    surreal_pre_source_backup_preserves_erasure_fence,
    snapshot_predating_source_keeps_journal_incarnation_fence
);

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
async fn erasure_retains_the_current_authorized_envelope(embedded: bool) {
    let client = client(embedded).await;
    let publication = SourcePublicationRepository::new(client.clone());
    let accepted = publication
        .accept(request("erase-a", "reclassified-before-erasure"))
        .await
        .unwrap();
    let sources = SourceRepository::new(client.clone());
    let mut changed = sources
        .get("erase-a", &accepted.source_id)
        .await
        .unwrap()
        .unwrap();
    changed.base.security.policy_id = "reclassified-private".into();
    changed.base.security.level = 5;
    changed.base.security.handling = vec!["no_external".into()];
    changed.base.subjects = vec![elephant_types::Link::new(RecordId::new_v4(
        TableName::Entity,
    ))];
    changed.base.subjects_state = elephant_types::records::SubjectsState::Partial;
    let changed = sources
        .update("erase-a", &accepted.source_id, changed)
        .await
        .unwrap();
    let expected = SourceAuthorizationEnvelope::from(&changed);
    publication
        .erase_live(mutation(&publication, &accepted, "erase-reclassified-source").await)
        .await
        .unwrap();
    assert_eq!(
        publication
            .authorization("erase-a", &accepted.source_id)
            .await
            .unwrap(),
        Some(expected.clone())
    );
    let events = crate::SystemEventRepository::new(client.clone())
        .find_recent_by_type_since(
            "source.erased",
            Utc::now() - chrono::Duration::minutes(1),
            10,
        )
        .await
        .unwrap();
    let event = events
        .iter()
        .find(|event| event.payload["source_id"] == serde_json::json!(accepted.source_id))
        .unwrap();
    assert_eq!(event.base.security, expected.security);
    assert_eq!(event.base.subjects, expected.subjects);
    assert_eq!(event.base.subjects_state, expected.subjects_state);
}
both_backends!(
    embedded_erasure_keeps_reclassified_authority,
    surreal_erasure_keeps_reclassified_authority,
    erasure_retains_the_current_authorized_envelope
);

#[test]
fn native_inventory_preserves_numeric_reference_object_identity() {
    let number_id = Thing::from(("consumer_note".to_string(), surrealdb::sql::Id::Number(42)));
    let string_id = Thing::from(("consumer_note", "42"));
    let native = crate::surreal::json_to_surreal(serde_json::json!({
        "reference": {"tb":"consumer_note", "id":42}
    }))
    .unwrap();
    let projected = inventory_json(native);
    assert_eq!(projected["reference"]["id"], serde_json::json!(42));
    assert!(contains_key_reference(
        &projected,
        &BTreeSet::from([number_id.to_string()])
    ));
    assert!(!contains_key_reference(
        &projected,
        &BTreeSet::from([string_id.to_string()])
    ));
}

#[test]
fn managed_asset_inventory_accepts_domain_and_native_uuid_references() {
    let asset = RecordId::new_v4(TableName::MediaAsset);
    let native = crate::surreal::thing_from_record_id(&asset);
    for reference in [asset.to_string(), native.to_string()] {
        assert_eq!(
            managed_asset_references(&serde_json::json!({"id":reference})).unwrap(),
            BTreeSet::from([asset.clone()])
        );
        assert_eq!(
            managed_asset_references(&serde_json::json!({"asset_id":reference})).unwrap(),
            BTreeSet::from([asset.clone()])
        );
    }
    assert!(managed_asset_references(&serde_json::json!({
        "asset_id":RecordId::new_v4(TableName::Entity).to_string()
    }))
    .is_err());
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
#[expect(
    clippy::too_many_lines,
    reason = "Keep the complete ordered conformance scenario and its failure assertions together."
)]
async fn pre_source_backup_restores_private_native_membership(embedded: bool) {
    use elephant_types::records::{ArtifactStatus, WorkArtifact};
    use surrealdb::sql::Id;
    let client = client(embedded).await;
    let directory = tempfile::tempdir().unwrap();
    let backup = directory.path().join("before-native-membership");
    crate::snapshot::export_named(
        &client,
        &backup,
        "before-native-membership",
        &crate::snapshot::SnapshotRetention::default(),
    )
    .await
    .unwrap();
    let publication = SourcePublicationRepository::new(client.clone());
    let accepted = publication
        .accept(request("erase-a", "private-native-membership"))
        .await
        .unwrap();
    let number_id = Thing::from(("consumer_note", Id::Number(42)));
    let row = serde_json::json!({"id":{"tb":"consumer_note","id":42},"space_id":"erase-a","source_id":accepted.source_id,"text":"private-payload"});
    if let Some(store) = client.embedded_store() {
        store
            .mutate(|tx| {
                tx.upsert_extension_row("consumer_note", row.clone())
                    .map_err(Into::into)
            })
            .await
            .unwrap();
    } else {
        let mut row = crate::surreal::json_to_surreal(row).unwrap();
        if let Value::Object(fields) = &mut row {
            fields.remove("id");
        }
        client
            .query_bind_checked(
                "CREATE $id CONTENT $row;",
                crate::bindings! {"id"=>number_id.clone(),"row"=>row},
            )
            .await
            .unwrap();
    }
    publication
        .erase_live(mutation(&publication, &accepted, "erase-private-native").await)
        .await
        .unwrap();
    let journal = client.erasure_journal().unwrap();
    let intent = journal
        .intents()
        .unwrap()
        .into_iter()
        .find(|intent| intent.source_id == accepted.source_id)
        .unwrap();
    let token = crate::reference_fence::reference_identity_token(
        journal.identity(),
        "erase-a",
        journal.reference_key_bytes(),
        &number_id,
    )
    .unwrap();
    assert!(journal
        .reference_membership(&intent)
        .unwrap()
        .contains(&token));
    crate::snapshot::restore_named(&client, &backup, journal)
        .await
        .unwrap();
    assert!(client.recovery_status().ready);
    assert!(publication
        .head("erase-a", &accepted.source_id)
        .await
        .unwrap()
        .is_none());
    let artifact = WorkArtifact {
        base: BaseFields::new(
            RecordId::new_v4(TableName::WorkArtifact),
            "erase-a".into(),
            "late-only-native-ref".into(),
        ),
        work_run: elephant_types::Link::new(RecordId::new_v4(TableName::WorkRun)),
        artifact_type: "only_native_identity".into(),
        data: serde_json::json!({"nested":serde_json::to_string(&serde_json::json!({"reference":{"tb":"consumer_note","id":42}})).unwrap()}),
        status: ArtifactStatus::Draft,
        validation_errors: None,
        validated_at: None,
        committed_at: None,
    };
    let artifacts = crate::WorkArtifactRepository::new(client.clone());
    assert!(artifacts.create(artifact.clone()).await.is_err());
    assert!(artifacts
        .get_including_deleted("erase-a", &artifact.base.id)
        .await
        .unwrap()
        .is_none());
    let mut independent = artifact;
    independent.base.id = RecordId::new_v4(TableName::WorkArtifact);
    independent.data = serde_json::json!({"reference":{"tb":"consumer_note","id":"42"}});
    artifacts.create(independent.clone()).await.unwrap();
    assert!(artifacts
        .get_including_deleted("erase-a", &independent.base.id)
        .await
        .unwrap()
        .is_some());
}

both_backends!(
    embedded_pre_source_backup_restores_native_membership,
    surreal_pre_source_backup_restores_native_membership,
    pre_source_backup_restores_private_native_membership
);

#[tokio::test]
#[expect(
    clippy::large_futures,
    reason = "Preserve the original future construction and polling shape exercised by this regression."
)]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
#[expect(
    clippy::too_many_lines,
    reason = "Keep the complete ordered conformance scenario and its failure assertions together."
)]
async fn surreal_reserved_inventory_refuses_independent_task_payload_and_owner_changes() {
    use surrealdb::sql::Id;
    let client = client(false).await;
    let publication = SourcePublicationRepository::new(client.clone());
    let accepted = publication
        .accept(request("erase-a", "reserved-native-race"))
        .await
        .unwrap();
    let owned = Thing::from(("consumer_note", Id::Number(42)));
    client.query_bind_checked("CREATE $id SET space_id='erase-a',source_id=$source,text='owned';", crate::bindings! {"id"=>owned.clone(),"source"=>crate::surreal::thing_from_record_id(&accepted.source_id)}).await.unwrap();
    // The same owner-change SQL is valid before the shared reservation.
    client
        .query_bind_checked(
            "UPDATE $id UNSET source_id;",
            crate::bindings! {"id"=>owned.clone()},
        )
        .await
        .unwrap();
    client.query_bind_checked("UPDATE $id SET source_id=$source;", crate::bindings! {"id"=>owned.clone(),"source"=>crate::surreal::thing_from_record_id(&accepted.source_id)}).await.unwrap();
    // This write has committed before admission and must enter the inventory.
    let early = Thing::from(("work_artifact", Id::Uuid(Uuid::new_v4().into())));
    client
        .query_bind_checked(
            "CREATE $id SET space_id='erase-a',reference=$owned,text='early';",
            crate::bindings! {"id"=>early.clone(),"owned"=>owned.clone()},
        )
        .await
        .unwrap();
    publication
        .erase_live_with_fault(
            mutation(&publication, &accepted, "erase-reserved-native-race").await,
            ErasureFault::AfterJournal,
        )
        .await
        .unwrap_err();
    let journal = client.erasure_journal().unwrap();
    let intent = journal
        .intents()
        .unwrap()
        .into_iter()
        .find(|intent| intent.source_id == accepted.source_id)
        .unwrap();
    let key = journal.reference_key_bytes().to_vec();
    let identity = journal.identity().to_owned();
    let database = client.db().unwrap().clone();
    let late = Thing::from(("work_artifact", Id::Uuid(Uuid::new_v4().into())));
    let late_check = late.clone();
    let owned_check = owned.clone();
    let early_check = early.clone();
    client
        .with_erasure_barrier(move |exclusive| async move {
            let repository = SourcePublicationRepository::new(exclusive.clone());
            exclusive
                .with_erasure_reservation(&intent, async {
                    crate::reference_fence::reserve_reference_guard(&exclusive, "erase-a", &intent)
                        .await?;
                    let observed =
                        crate::reference_fence::reference_guard_revision(&exclusive, "erase-a")
                            .await?;
                    let head = repository.head("erase-a", &intent.source_id).await?;
                    let plan = repository
                        .prepare_lifecycle_plan("erase-a", &intent.source_id, true)
                        .await?;
                    assert!(plan
                        .deletes
                        .iter()
                        .any(|row| row.native_id.as_ref() == Some(&early)));
                    // The raw SDK clone bypasses this StorageClient's in-memory barrier,
                    // like a distinct remote client. Tokio spawn does not inherit the
                    // admitted owner's task-local capability.
                    let changes = tokio::spawn(async move {
                        let mut before = database.query("SELECT * FROM ONLY $id;").bind(("id",owned.clone())).await.unwrap().check().unwrap();
                        let before: surrealdb::Value = before.take(0).unwrap();
                        let late_write = database
                            .query(
                                "CREATE $id SET space_id='erase-a',reference=$owned,text='late';",
                            )
                            .bind(("id", late.clone()))
                            .bind(("owned", owned.clone()))
                            .bind(("__recall_reference_key", Value::Bytes(key.clone().into())))
                            .bind(("__recall_journal_identity", identity.clone()))
                            .bind(("__recall_erasure_reservation", Value::None))
                            .await
                            .unwrap()
                            .check();
                        assert!(matches!(late_write, Err(surrealdb::Error::Db(surrealdb::error::Db::Thrown(ref message))) if message == "publication:erasure_space_reserved"));
                        let owner_change = database
                            .query("UPDATE $id UNSET source_id;")
                            .bind(("id", owned.clone()))
                            .bind(("__recall_reference_key", Value::Bytes(key.into())))
                            .bind(("__recall_journal_identity", identity))
                            .bind(("__recall_erasure_reservation", Value::None))
                            .await
                            .unwrap()
                            .check();
                        assert!(matches!(owner_change, Err(surrealdb::Error::Db(surrealdb::error::Db::Thrown(ref message))) if message == "publication:erasure_space_reserved"));
                        let mut after = database.query("SELECT * FROM ONLY $id;").bind(("id",owned)).await.unwrap().check().unwrap();
                        let after: surrealdb::Value = after.take(0).unwrap();
                        assert_eq!(before.into_inner(), after.into_inner(), "refused ownership change must leave the complete row unchanged");
                        let mut absent = database.query("SELECT * FROM ONLY $id;").bind(("id",late)).await.unwrap().check().unwrap();
                        let absent: surrealdb::Value = absent.take(0).unwrap();
                        assert!(matches!(absent.into_inner(), Value::None), "refused late write must not create any row");
                    });
                    changes.await.unwrap();
                    let memberships =
                        repository.persist_plan_reference_membership(&intent, &plan)?;
                    repository
                        .commit_physical_purge(
                            "erase-a",
                            &intent.source_id,
                            &intent.intent_id,
                            &plan,
                            head.as_ref(),
                            &memberships,
                            &observed,
                            ErasureFault::None,
                        )
                        .await?;
                    repository
                        .remove_managed_erasure_files("erase-a", &intent.source_id)
                        .await?;
                    exclusive.persist_restored_state().await?;
                    repository
                        .complete_erasure("erase-a", &intent.source_id, &plan.event_records)
                        .await?;
                    Ok(())
                })
                .await
        })
        .await
        .unwrap();
    assert!(client.recovery_status().ready);
    let rows = publication.inventory("erase-a").await.unwrap();
    assert!(rows
        .iter()
        .all(|row| row.native_id.as_ref().is_none_or(|id| ![
            &early_check,
            &late_check,
            &owned_check
        ]
        .contains(&id))));
    assert!(crate::reference_fence::reference_reservations(&client)
        .await
        .unwrap()
        .is_empty());
}

#[test]
fn physical_uuid_reference_matching_preserves_prose_and_extension_identity() {
    let source = RecordId::new_v4(TableName::Source);
    let removed = BTreeSet::from([source.id()]);
    for value in [
        serde_json::json!(source.to_string()),
        serde_json::json!(crate::surreal::thing_from_record_id(&source).to_string()),
        serde_json::json!(source.id().to_string()),
        serde_json::json!(source.id().simple().to_string()),
        serde_json::json!(serde_json::to_string(&serde_json::json!({"source":source})).unwrap()),
        serde_json::json!({"lookup":{source.to_string():true}}),
    ] {
        assert!(
            contains_reference(&value, &removed),
            "exact reference was missed"
        );
    }
    for value in [
        serde_json::json!(format!("a sentence mentions {source} as ordinary text")),
        serde_json::json!(format!("prefix-{}-suffix", source.id())),
        serde_json::json!(format!("{source} /* trailing prose */")),
        serde_json::json!({"note":format!("https://example.invalid/{}",source.id())}),
        serde_json::json!({"tb":"consumer_note","id":source.id().to_string()}),
        serde_json::json!(
            Thing::from(("consumer_note", source.id().to_string().as_str())).to_string()
        ),
    ] {
        assert!(
            !contains_reference(&value, &removed),
            "prose or unrelated extension identity became ownership"
        );
    }
}

#[expect(
    clippy::large_futures,
    reason = "Preserve the original future construction and polling shape exercised by this regression."
)]
async fn reserved_backup_replays_prior_membership_before_independent_inverse(embedded: bool) {
    decision_inverse_with_prior_erasure(embedded, true).await;
}

both_backends!(
    embedded_reserved_backup_replays_prior_membership_before_inverse,
    surreal_reserved_backup_replays_prior_membership_before_inverse,
    reserved_backup_replays_prior_membership_before_independent_inverse
);

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
async fn ordinary_erasure_event_labels_do_not_authorize_payload_retention(embedded: bool) {
    let client = client(embedded).await;
    let publication = SourcePublicationRepository::new(client.clone());
    let accepted = publication
        .accept(request("erase-a", "ordinary-erased-event-label"))
        .await
        .unwrap();
    let events = crate::SystemEventRepository::new(client.clone());
    let forged = SystemEvent {
        base: BaseFields::new(
            RecordId::new_v4(TableName::SystemEvent),
            "erase-a".into(),
            "ordinary-producer".into(),
        ),
        seq: events.get_latest_seq("erase-a").await.unwrap() + 1,
        event_type: "source.erased".into(),
        payload: serde_json::json!({
            "source_id": accepted.source_id,
            "text": "ERASE-ME-ordinary-event-payload",
        }),
    };
    // The normal repository grammar succeeds before erase. The label alone
    // cannot promote this content-bearing row to retained lifecycle control.
    events.create(forged.clone()).await.unwrap();
    assert!(events.get_by_id(&forged.base.id).await.unwrap().is_some());
    publication
        .erase_live(mutation(&publication, &accepted, "erase-ordinary-event-label").await)
        .await
        .unwrap();
    assert!(client.recovery_status().ready);
    assert!(events.get_by_id(&forged.base.id).await.unwrap().is_none());
    let inventory = publication.inventory("erase-a").await.unwrap();
    let completion = inventory
        .iter()
        .find(|row| {
            row.table == "system_event"
                && row
                    .value
                    .get("event_type")
                    .and_then(serde_json::Value::as_str)
                    == Some("source.erased")
        })
        .expect("actual payload-free completion remains");
    assert!(retained_erasure_completions("erase-a", &inventory)
        .unwrap()
        .contains(&completion.physical_key()));
    // Persisted old events also need exact content validation. Even the true
    // event's ID and operation receipt cannot authorize an added payload field.
    let mut tampered_inventory = inventory.clone();
    tampered_inventory
        .iter_mut()
        .find(|row| row.physical_key() == completion.physical_key())
        .unwrap()
        .value["payload"]["text"] = "ERASE-ME-forged-completion".into();
    assert!(
        !retained_erasure_completions("erase-a", &tampered_inventory)
            .unwrap()
            .contains(&completion.physical_key())
    );
    assert!(!inventory.iter().any(|row| {
        row.value
            .to_string()
            .contains("ERASE-ME-ordinary-event-payload")
    }));

    let mut late = forged;
    late.base.id = RecordId::new_v4(TableName::SystemEvent);
    late.seq = events.get_latest_seq("erase-a").await.unwrap() + 1;
    let error = events.create(late.clone()).await.unwrap_err();
    assert!(error.to_string().contains("source_erased"), "{error}");
    assert!(events.get_by_id(&late.base.id).await.unwrap().is_none());
}

both_backends!(
    embedded_erasure_event_labels_do_not_retain_payload,
    surreal_erasure_event_labels_do_not_retain_payload,
    ordinary_erasure_event_labels_do_not_authorize_payload_retention
);

#[test]
fn native_inventory_preserves_composite_component_types_in_reference_comparisons() {
    use surrealdb::sql::{Id, Object};
    let directory = tempfile::tempdir().unwrap();
    let journal = crate::erasure_journal::ErasureJournal::open(directory.path()).unwrap();
    let composite = Value::Array(vec![Value::from(1.25_f64), Value::from(42_i64)].into());
    let Value::Array(component_values) = composite.clone() else {
        unreachable!();
    };
    let erased = Thing::from(("consumer_extension", Id::Array(component_values)));
    let carrier = Thing::from(("consumer_note", "carrier"));
    let native = Value::Object(Object::from(BTreeMap::<String, Value>::from([
        ("id".into(), Value::Thing(carrier.clone())),
        ("space_id".into(), Value::from("erase-a")),
        (
            "reference".into(),
            Value::Object(Object::from(BTreeMap::<String, Value>::from([
                ("tb".into(), Value::from("consumer_extension")),
                ("id".into(), composite),
            ]))),
        ),
    ])));
    let token = crate::reference_fence::reference_identity_token(
        journal.identity(),
        "erase-a",
        journal.reference_key_bytes(),
        &erased,
    )
    .unwrap();
    let membership = BTreeSet::from([token]);
    let mut row = InventoryRow {
        table: "consumer_note".into(),
        key: carrier.to_string(),
        native_id: Some(carrier),
        record_id: None,
        value: inventory_json(native.clone()),
        native_value: None,
    };
    assert!(
        !row.intersects_membership(&journal, "erase-a", &membership)
            .unwrap(),
        "the conservative JSON projection intentionally loses native Float identity type"
    );
    row.native_value = Some(native);
    assert!(row
        .intersects_membership(&journal, "erase-a", &membership)
        .unwrap());
    // Production closure mixes exact extension IDs with normalized core and
    // control UUID keys. Never reparse these display keys as native SQL IDs.
    let identities = [
        erased,
        crate::surreal::thing_from_record_id(&RecordId::new_v4(TableName::Source)),
        Thing::from((
            "source_decision_receipt",
            Id::String(Uuid::new_v4().to_string()),
        )),
    ];
    let keys = identities.iter().map(normalized_native_key).collect();
    let tokens = identities
        .iter()
        .map(|identity| {
            crate::reference_fence::reference_identity_token(
                journal.identity(),
                "erase-a",
                journal.reference_key_bytes(),
                identity,
            )
            .unwrap()
        })
        .collect();
    assert!(row
        .contains_key_reference(&journal, "erase-a", &keys, &tokens)
        .unwrap());
    let Value::Object(mut independent) = row.native_value.clone().unwrap() else {
        unreachable!();
    };
    independent
        .0
        .insert("reference".into(), Value::from("independent content"));
    row.native_value = Some(Value::Object(independent));
    row.value = inventory_json(row.native_value.clone().unwrap());
    assert!(
        !row.contains_key_reference(&journal, "erase-a", &keys, &tokens)
            .unwrap(),
        "unrelated rows survive a closure containing normalized UUID keys"
    );
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
async fn surreal_restore_purges_historical_declared_native_composite_reference() {
    use surrealdb::sql::{Id, Object};
    let client = client(false).await;
    let publication = SourcePublicationRepository::new(client.clone());
    let erased = Thing::from((
        "consumer_extension",
        Id::Array(vec![Value::from(1.25_f64), Value::from(42_i64)].into()),
    ));
    let carrier = Thing::from(("consumer_note", "historical-composite-reference"));
    let independent = Thing::from(("consumer_note", "independent-string-composite"));
    for (id, component) in [
        (carrier.clone(), Value::from(1.25_f64)),
        (independent.clone(), Value::from("1.25f")),
    ] {
        let reference = Value::Object(Object::from(BTreeMap::<String, Value>::from([
            ("tb".into(), Value::from("consumer_extension")),
            (
                "id".into(),
                Value::Array(vec![component, Value::from(42_i64)].into()),
            ),
        ])));
        client
            .query_bind_checked(
                "CREATE $id SET space_id='erase-a',reference=$reference;",
                crate::bindings! {"id"=>id,"reference"=>reference},
            )
            .await
            .unwrap();
    }
    let directory = tempfile::tempdir().unwrap();
    let backup = directory.path().join("before-native-composite-source");
    crate::snapshot::export_named(
        &client,
        &backup,
        "before-native-composite-source",
        &crate::snapshot::SnapshotRetention::default(),
    )
    .await
    .unwrap();
    let accepted = publication
        .accept(request("erase-a", "native-composite-source"))
        .await
        .unwrap();
    client.query_bind_checked(
        "CREATE $id SET space_id='erase-a',source_id=$source,text='ERASE-ME-composite';",
        crate::bindings! {"id"=>erased.clone(),"source"=>crate::surreal::thing_from_record_id(&accepted.source_id)},
    ).await.unwrap();
    publication
        .erase_live(mutation(&publication, &accepted, "erase-native-composite").await)
        .await
        .unwrap();
    let journal = client.erasure_journal().unwrap();
    let intent = journal
        .intents()
        .unwrap()
        .into_iter()
        .find(|intent| intent.source_id == accepted.source_id)
        .unwrap();
    let membership = journal.reference_membership(&intent).unwrap();
    assert!(membership.contains(
        &crate::reference_fence::reference_identity_token(
            journal.identity(),
            "erase-a",
            journal.reference_key_bytes(),
            &carrier
        )
        .unwrap()
    ));
    assert!(!membership.contains(
        &crate::reference_fence::reference_identity_token(
            journal.identity(),
            "erase-a",
            journal.reference_key_bytes(),
            &independent
        )
        .unwrap()
    ));
    crate::snapshot::restore_named(&client, &backup, journal)
        .await
        .unwrap();
    assert!(client.recovery_status().ready);
    assert!(publication
        .head("erase-a", &accepted.source_id)
        .await
        .unwrap()
        .is_none());
    let rows = publication.inventory("erase-a").await.unwrap();
    assert!(!rows
        .iter()
        .any(|row| row.native_id.as_ref() == Some(&carrier)));
    assert!(rows
        .iter()
        .any(|row| row.native_id.as_ref() == Some(&independent)));
}

#[expect(
    clippy::large_futures,
    reason = "Preserve the original future construction and polling shape exercised by this regression."
)]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
#[expect(
    clippy::too_many_lines,
    reason = "Keep the complete ordered conformance scenario and its failure assertions together."
)]
async fn retraction_checks_all_original_document_observations(embedded: bool) {
    use crate::source_document_guards::PreparedDocGuards;
    use elephant_types::records::Doc;
    use elephant_types::Link;

    let storage = client(embedded).await;
    let publication = SourcePublicationRepository::new(storage.clone());
    let receipt = publication
        .accept(request("erase-a", "guarded-retraction"))
        .await
        .unwrap();
    let docs = crate::DocRepository::new(storage.clone());
    let document = |title: &str| Doc {
        base: BaseFields::new(
            RecordId::new_v4(TableName::Doc),
            "erase-a".into(),
            "test".into(),
        ),
        title: title.into(),
        text: format!("original {title}"),
        sidecar_text: None,
        source: Some(Link::new(receipt.source_id.clone())),
        media_attachments: vec![],
        rendition_key: title.into(),
        metadata: serde_json::json!({}),
    };
    let caller = docs.create(document("caller")).await.unwrap();
    let current = docs.create(document("current")).await.unwrap();
    let head = publication
        .head("erase-a", &receipt.source_id)
        .await
        .unwrap()
        .unwrap();
    let guards = PreparedDocGuards::from_authorized_documents(
        "erase-a",
        &receipt.source_id,
        Some(&head),
        &[caller.clone(), current.clone()],
    )
    .unwrap();
    let mut mutation = mutation(&publication, &receipt, "retract-all-documents").await;
    mutation.authorized_document = Some(caller.clone());
    let mut changed = current.clone();
    changed.text = "changed after authorization".into();
    let changed = docs
        .update("erase-a", &current.base.id, changed)
        .await
        .unwrap();
    assert!(publication
        .retract_with_document_guards(mutation.clone(), &guards)
        .await
        .is_err());
    assert_eq!(
        publication
            .head("erase-a", &receipt.source_id)
            .await
            .unwrap(),
        Some(head.clone())
    );
    assert!(publication
        .operation_by_idempotency_key("erase-a", &mutation.idempotency_key)
        .await
        .unwrap()
        .is_none());
    assert_eq!(
        docs.get("erase-a", &caller.base.id).await.unwrap(),
        Some(caller.clone())
    );
    assert_eq!(
        docs.get("erase-a", &changed.base.id).await.unwrap(),
        Some(changed.clone())
    );

    let pending = document("not-yet-staged");
    let fresh = PreparedDocGuards::from_authorized_documents(
        "erase-a",
        &receipt.source_id,
        Some(&head),
        &[caller.clone(), changed.clone()],
    )
    .unwrap()
    .with_absent_documents(std::slice::from_ref(&pending.base.id))
    .unwrap();
    let appeared = docs.create(pending).await.unwrap();
    assert!(publication
        .retract_with_document_guards(mutation.clone(), &fresh)
        .await
        .is_err());
    assert_eq!(
        publication
            .head("erase-a", &receipt.source_id)
            .await
            .unwrap(),
        Some(head.clone())
    );
    assert!(publication
        .operation_by_idempotency_key("erase-a", &mutation.idempotency_key)
        .await
        .unwrap()
        .is_none());
    let authorized = PreparedDocGuards::from_authorized_documents(
        "erase-a",
        &receipt.source_id,
        Some(&head),
        &[caller, changed, appeared],
    )
    .unwrap();
    let retracted = publication
        .retract_with_document_guards(mutation.clone(), &authorized)
        .await
        .unwrap();
    assert_eq!(retracted.state, OperationState::Retracted);
    assert_eq!(
        publication
            .retract_with_document_guards(mutation, &guards)
            .await
            .unwrap(),
        retracted,
        "original receipt replay is read-only before stale head/document guards"
    );
}

both_backends!(
    embedded_retraction_checks_all_original_documents,
    surreal_retraction_checks_all_original_documents,
    retraction_checks_all_original_document_observations
);

#[expect(
    clippy::large_futures,
    reason = "Preserve the original future construction and polling shape exercised by this regression."
)]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
#[expect(
    clippy::too_many_lines,
    reason = "Keep the complete ordered conformance scenario and its failure assertions together."
)]
async fn retraction_preserves_original_aba_acceptance_and_guards_prior_operation(embedded: bool) {
    let storage = client(embedded).await;
    let publication = SourcePublicationRepository::new(storage.clone());
    let original = request("erase-a", "retraction-original-aba");
    let first = publication.accept(original.clone()).await.unwrap();
    let mut second_request = original.clone();
    second_request.revision_id = "r2".into();
    second_request.source.hash = "second-content".into();
    second_request.idempotency_key = "accept-aba-b".into();
    second_request.request_fingerprint = "accept-aba-b".into();
    second_request.expected = SourcePrecondition::Generation(first.source_generation.clone());
    second_request.authorized = publication
        .authorization_snapshot("erase-a", &first.source_id)
        .await
        .unwrap();
    let second = publication.accept(second_request).await.unwrap();
    let mut third_request = original;
    third_request.idempotency_key = "accept-aba-a-again".into();
    third_request.request_fingerprint = "accept-aba-a-again".into();
    third_request.expected = SourcePrecondition::Generation(second.source_generation);
    third_request.authorized = publication
        .authorization_snapshot("erase-a", &first.source_id)
        .await
        .unwrap();
    let third = publication.accept(third_request).await.unwrap();
    assert_ne!(third.operation_id, first.operation_id);
    assert_ne!(third.source_generation, first.source_generation);
    assert_eq!(third.desired_revision, first.desired_revision);
    assert_eq!(
        third.state,
        OperationState::Accepted,
        "pending acceptance is valid original lineage"
    );
    let expected = PublicationFence {
        source_id: third.source_id.clone(),
        generation: third.source_generation.clone(),
        revision_id: "r1".into(),
        operation_id: third.operation_id.clone(),
    };
    let request = mutation(&publication, &third, "aba-first-retraction").await;
    let reservation = publication
        .prepare_lifecycle(&request, MutationKind::Retract)
        .await
        .unwrap();
    assert_eq!(
        reservation.operation.predecessor_publication.as_ref(),
        Some(&expected)
    );
    let plan = publication
        .prepare_lifecycle_plan("erase-a", &third.source_id, false)
        .await
        .unwrap();
    let (mut changed, _) = publication
        .lifecycle_operation_snapshot("erase-a", &third.operation_id)
        .await
        .unwrap();
    changed
        .request_fingerprint
        .push_str("-concurrently-changed");
    write_lifecycle_operation_fixture(
        &storage,
        "erase-a",
        &third.source_id,
        &third.operation_id,
        &changed,
    )
    .await;
    let before = publication.head("erase-a", &third.source_id).await.unwrap();
    let error = publication
        .commit_lifecycle_reservation(&request, &reservation, Some(&plan), None)
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("lifecycle_original_operation_changed"),
        "{error}"
    );
    assert_eq!(
        publication.head("erase-a", &third.source_id).await.unwrap(),
        before
    );
    assert!(publication
        .operation_by_idempotency_key("erase-a", &request.idempotency_key)
        .await
        .unwrap()
        .is_none());
    let original_operation: StoredOperation =
        decode_payload(&reservation.observed_operations[0].1).unwrap();
    write_lifecycle_operation_fixture(
        &storage,
        "erase-a",
        &third.source_id,
        &third.operation_id,
        &original_operation,
    )
    .await;
    let retracted = publication.retract(request.clone()).await.unwrap();
    let retracted_head = publication
        .head("erase-a", &third.source_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        publication
            .publication_before_retraction("erase-a", &retracted_head)
            .await
            .unwrap(),
        expected
    );

    let again_request = mutation(&publication, &retracted, "aba-repeated-retraction").await;
    let repeated = publication.retract(again_request).await.unwrap();
    let repeated_head = publication
        .head("erase-a", &third.source_id)
        .await
        .unwrap()
        .unwrap();
    assert_ne!(repeated.operation_id, retracted.operation_id);
    assert_eq!(
        publication
            .publication_before_retraction("erase-a", &repeated_head)
            .await
            .unwrap(),
        expected
    );
    assert_eq!(
        publication.retract(request).await.unwrap(),
        retracted,
        "historical exact retry remains read-only"
    );
    assert!(publication
        .publication_before_retraction("erase-b", &repeated_head)
        .await
        .is_err());
    assert!(publication
        .publication_before_retraction("erase-a", &retracted_head)
        .await
        .is_err());

    // Historical rows which lack original lineage are unknown, never repaired
    // from the first acceptance stored alongside a reusable immutable revision.
    let (mut unknown, _) = publication
        .lifecycle_operation_snapshot("erase-a", &repeated.operation_id)
        .await
        .unwrap();
    unknown.predecessor_publication = None;
    write_lifecycle_operation_fixture(
        &storage,
        "erase-a",
        &third.source_id,
        &repeated.operation_id,
        &unknown,
    )
    .await;
    assert!(publication
        .publication_before_retraction("erase-a", &repeated_head)
        .await
        .is_err());
    let unknown_request = mutation(&publication, &repeated, "unknown-repeated-retraction").await;
    assert!(publication.retract(unknown_request.clone()).await.is_err());
    assert_eq!(
        publication.head("erase-a", &third.source_id).await.unwrap(),
        Some(repeated_head)
    );
    assert!(publication
        .operation_by_idempotency_key("erase-a", &unknown_request.idempotency_key)
        .await
        .unwrap()
        .is_none());
}

async fn write_lifecycle_operation_fixture(
    storage: &StorageClient,
    space: &str,
    source: &RecordId,
    key: &str,
    operation: &StoredOperation,
) {
    if let Some(store) = storage.embedded_store() {
        store
            .mutate(|tx| write_control_tx(tx, OPERATION_TABLE, key, space, source, operation))
            .await
            .unwrap();
    } else {
        storage.query_bind_checked(
            "UPDATE $id CONTENT $record;",
            crate::bindings! { "id" => control_thing(OPERATION_TABLE, key), "record" => control_value(space, source, operation).unwrap() },
        ).await.unwrap();
    }
}

both_backends!(
    embedded_retraction_keeps_original_aba_acceptance,
    surreal_retraction_keeps_original_aba_acceptance,
    retraction_preserves_original_aba_acceptance_and_guards_prior_operation
);

#[test]
fn new_continuation_rows_obey_current_and_historical_erasure_closure() {
    use surrealdb::sql::{Id, Object};

    let directory = tempfile::tempdir().unwrap();
    let journal = crate::erasure_journal::ErasureJournal::open(directory.path()).unwrap();
    let source = RecordId::new_v4(TableName::Source);
    let survivor = RecordId::new_v4(TableName::Source);
    let erased_receipt = control_thing("source_commit_receipt", &Uuid::new_v4().to_string());
    let composite = Thing::from((
        "consumer_extension",
        Id::Array(vec![Value::from(1.25_f64), Value::from(42_i64)].into()),
    ));
    let token = |id: &Thing| {
        crate::reference_fence::reference_identity_token(
            journal.identity(),
            "erase-a",
            journal.reference_key_bytes(),
            id,
        )
        .unwrap()
    };
    let plan = LifecyclePlan {
        removed: BTreeSet::from([source.id()]),
        removed_keys: BTreeSet::from([normalized_native_key(&erased_receipt)]),
        removed_identity_tokens: BTreeSet::from([token(&erased_receipt)]),
        ..LifecyclePlan::default()
    };
    let historical = BTreeSet::from([token(&composite)]);
    let make_row = |payload: serde_json::Value, reference: Option<Value>| {
        let id = control_thing("source_commit_receipt", &Uuid::new_v4().to_string());
        let Value::Object(mut fields) = control_value("erase-a", &survivor, &payload).unwrap()
        else {
            unreachable!();
        };
        if let Some(reference) = reference {
            fields.insert("reference".into(), reference);
        }
        let native = Value::Object(fields);
        InventoryRow {
            table: id.tb.clone(),
            key: normalized_native_key(&id),
            native_id: Some(id),
            record_id: None,
            value: inventory_json(native.clone()),
            native_value: Some(native),
        }
    };
    let valid = make_row(
        serde_json::json!({"source": survivor, "fact": "independent survivor"}),
        None,
    );
    assert!(!valid
        .references_erased_identity(&journal, "erase-a", &plan, &historical)
        .unwrap());
    for forbidden in [
        make_row(serde_json::json!({"before": {"source": source}}), None),
        make_row(serde_json::json!({}), Some(Value::Thing(erased_receipt))),
        make_row(
            serde_json::json!({}),
            Some(Value::Object(Object::from(
                BTreeMap::<String, Value>::from([
                    ("tb".into(), Value::from("consumer_extension")),
                    (
                        "id".into(),
                        Value::Array(vec![Value::from(1.25_f64), Value::from(42_i64)].into()),
                    ),
                ]),
            ))),
        ),
    ] {
        assert!(forbidden
            .references_erased_identity(&journal, "erase-a", &plan, &historical)
            .unwrap());
    }
    let independent_text = make_row(
        serde_json::json!({"text": format!("independent prose mentioning {source}")}),
        Some(Value::Object(Object::from(
            BTreeMap::<String, Value>::from([
                ("tb".into(), Value::from("consumer_extension")),
                (
                    "id".into(),
                    Value::Array(vec![Value::from("1.25f"), Value::from(42_i64)].into()),
                ),
            ]),
        ))),
    );
    assert!(!independent_text
        .references_erased_identity(&journal, "erase-a", &plan, &historical)
        .unwrap());
}

#[path = "source_lifecycle_standing_watch_tests.rs"]
mod standing_watch_tests;
