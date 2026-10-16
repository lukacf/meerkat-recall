//! Actual acceptance and transaction guards for sealed contact authority.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use super::*;
use crate::{Repository, StorageConfig};
use chrono::Utc;
use elephant_types::publication::{SourcePrecondition, SourceVisibility};
use elephant_types::records::{
    base::SubjectsState, entity::EntityType, BaseFields, Entity, Source, SourceType, SpaceConfig,
};
use elephant_types::TableName;

async fn client(embedded: bool) -> StorageClient {
    let storage = if embedded {
        StorageClient::new_embedded().await
    } else {
        StorageClient::new(StorageConfig::default()).await
    }
    .unwrap();
    if !embedded {
        for schema in [
            concat!(
                include_str!("../../migrations/sql/031_source_publication.surql"),
                "\n",
                include_str!("../../migrations/sql/046_source_support_index_fence.surql")
            ),
            include_str!("../../migrations/sql/039_private_reference_fence.surql"),
            include_str!("../../migrations/sql/040_source_contact_intent.surql"),
        ] {
            storage.query_checked(schema).await.unwrap();
        }
    }
    crate::SpaceConfigRepository::new(storage.clone())
        .create(SpaceConfig {
            base: base(TableName::SpaceConfig),
            embedding_provider: "fake".into(),
            embedding_model: "fixture".into(),
            embedding_dims: 8,
            processing: None,
            purpose: None,
            skill: None,
        })
        .await
        .unwrap();
    storage
}
fn base(table: TableName) -> BaseFields {
    let mut base = BaseFields::new(
        RecordId::new_v4(table),
        "contact-space".into(),
        "test".into(),
    );
    base.subjects_state = SubjectsState::Complete;
    base
}
fn request() -> AcceptSourceRevision {
    let mut source = Source::new(
        base(TableName::Source),
        SourceType::Manual,
        "contact-content".into(),
    );
    source.external_id = Some("retained-contact".into());
    source.metadata = serde_json::json!({"contact":{"name":"Ada Fixture","email":"ADA@example.invalid","company":"Example Company","job_title":"Engineer"}});
    AcceptSourceRevision {
        source,
        revision_id: "contact-r1".into(),
        expected: SourcePrecondition::Absent,
        idempotency_key: "contact-intent-1".into(),
        request_fingerprint: "contact-semantic-1".into(),
        reactivate: false,
        recreate_after_erasure: false,
        authorized: None,
        authorized_document: None,
    }
}
fn publication(receipt: &OperationReceipt) -> PublicationFence {
    PublicationFence {
        source_id: receipt.source_id.clone(),
        generation: receipt.source_generation.clone(),
        revision_id: receipt.desired_revision.clone().unwrap(),
        operation_id: receipt.operation_id.clone(),
    }
}
async fn company(storage: &StorageClient) -> Entity {
    crate::EntityRepository::new(storage.clone())
        .create(Entity::new(
            base(TableName::Entity),
            EntityType::new("organization"),
            "Example Company".into(),
        ))
        .await
        .unwrap()
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture, repository, and mutation owners alive through the original conformance phase."
)]
#[expect(
    clippy::too_many_lines,
    reason = "Keep the complete fault and recovery sequence together with its original assertions."
)]
async fn original_retry_and_atomic_admission(embedded: bool) {
    let storage = client(embedded).await;
    let repo = SourcePublicationRepository::new(storage.clone());
    let request = request();
    let contact = PreparedSourceContact::prepare(&storage, &request.source)
        .await
        .unwrap()
        .unwrap();
    let receipt = repo
        .accept_with_prepared_contact(request.clone(), &contact)
        .await
        .unwrap();
    assert_eq!(receipt.state, OperationState::Accepted);
    assert_eq!(receipt.published_revision, None);
    let state = AcceptedContactState::load(&storage, "contact-space", &publication(&receipt))
        .await
        .unwrap();
    for lifecycle_state in [
        OperationState::Retracted,
        OperationState::Erasing,
        OperationState::Erased,
    ] {
        let mut invalid = state.clone();
        invalid.operation.receipt.state = lifecycle_state;
        assert!(
            invalid.validate().is_err(),
            "lifecycle identity cannot impersonate an original acceptance"
        );
    }
    assert_eq!(
        state.operation.contact_digest.as_deref(),
        Some(contact.digest())
    );
    assert_eq!(
        state.intent.as_ref().unwrap().contact.digest(),
        contact.digest()
    );
    // Acceptance persists authority; graph effects/outbox belong to finalization.
    for record in contact.authorizations() {
        let record = record.canonical();
        assert!(
            CanonicalRecord::load(&storage, "contact-space", &record.base().id)
                .await
                .unwrap()
                .is_none()
        );
    }
    assert!(
        repo.accept(request.clone()).await.is_err(),
        "same key cannot lose its contact capability"
    );
    company(&storage).await;
    company(&storage).await;
    assert!(
        PreparedSourceContact::prepare(&storage, &request.source)
            .await
            .is_err(),
        "latest selector is now ambiguous"
    );
    let original = repo
        .prepared_contact_retry(&request)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        original.digest(),
        contact.digest(),
        "retry keeps original sealed authority"
    );
    assert_eq!(
        repo.accept_with_prepared_contact(request.clone(), &original)
            .await
            .unwrap(),
        receipt
    );
    let mut changed = request.clone();
    changed.source.metadata["contact"]["job_title"] = "Changed semantic input".into();
    assert!(repo.prepared_contact_retry(&changed).await.is_err());
    let mut generic = request.clone();
    generic.idempotency_key = "generic-operation".into();
    generic.expected = SourcePrecondition::Generation(receipt.source_generation.clone());
    generic.authorized = repo
        .authorization_snapshot("contact-space", &receipt.source_id)
        .await
        .unwrap();
    let generic_receipt = repo.accept(generic.clone()).await.unwrap();
    assert!(matches!(
        repo.source_acceptance_retry(&generic).await.unwrap(),
        SourceAcceptanceRetry::Existing { contact: None, operation }
            if operation == generic_receipt
    ));
    assert!(
        repo.prepared_reprocess_contact(&generic, &publication(&receipt))
            .await
            .unwrap()
            .is_none(),
        "plain retry remains plain after its original document fence advanced"
    );
    let mut new_intent = generic.clone();
    new_intent.idempotency_key = "not-yet-admitted".into();
    assert!(matches!(
        repo.source_acceptance_retry(&new_intent).await.unwrap(),
        SourceAcceptanceRetry::NewIntent
    ));
    assert!(
        repo.prepared_reprocess_contact(&new_intent, &publication(&receipt))
            .await
            .is_err(),
        "a new intent cannot bypass the stale original document fence"
    );
    assert_eq!(
        repo.prepared_reprocess_contact(&request, &publication(&receipt))
            .await
            .unwrap()
            .unwrap()
            .digest(),
        contact.digest(),
        "sealed retry retains the original plan despite newer generic head and ambiguous selectors"
    );
    assert!(
        repo.accept_with_prepared_contact(generic, &original)
            .await
            .is_err(),
        "same generic key cannot gain a contact capability"
    );
    assert_ne!(generic_receipt.source_generation, receipt.source_generation);
}

#[tokio::test]
#[expect(
    clippy::large_futures,
    reason = "Keep the conformance helper's original await and allocation shape."
)]
async fn embedded_contact_retry_preserves_original_authority() {
    original_retry_and_atomic_admission(true).await;
}
#[tokio::test]
#[expect(
    clippy::large_futures,
    reason = "Keep the conformance helper's original await and allocation shape."
)]
async fn surreal_contact_retry_preserves_original_authority() {
    original_retry_and_atomic_admission(false).await;
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture, repository, and mutation owners alive through the original conformance phase."
)]
async fn stale_admission_leaves_no_source_or_receipt(embedded: bool) {
    let storage = client(embedded).await;
    let repo = SourcePublicationRepository::new(storage.clone());
    let request = request();
    let contact = PreparedSourceContact::prepare(&storage, &request.source)
        .await
        .unwrap()
        .unwrap();
    company(&storage).await;
    let before = CanonicalRepository::new(storage.clone())
        .epoch("contact-space")
        .await
        .unwrap();
    assert!(repo
        .accept_with_prepared_contact(request.clone(), &contact)
        .await
        .is_err());
    assert!(repo
        .operation_by_idempotency_key("contact-space", &request.idempotency_key)
        .await
        .unwrap()
        .is_none());
    assert!(repo
        .head("contact-space", &request.source.base.id)
        .await
        .unwrap()
        .is_none());
    assert!(repo
        .revision(
            "contact-space",
            &request.source.base.id,
            &request.revision_id
        )
        .await
        .unwrap()
        .is_none());
    assert!(crate::SourceRepository::new(storage.clone())
        .get("contact-space", &request.source.base.id)
        .await
        .unwrap()
        .is_none());
    assert!(repo
        .list_controls::<AcceptedSourceContact>(CONTACT_TABLE, Some("contact-space"))
        .await
        .unwrap()
        .is_empty());
    assert_eq!(
        CanonicalRepository::new(storage.clone())
            .epoch("contact-space")
            .await
            .unwrap(),
        before
    );
}
#[tokio::test]
#[expect(
    clippy::large_futures,
    reason = "Keep the conformance helper's original await and allocation shape."
)]
async fn embedded_stale_contact_admission_is_atomic() {
    stale_admission_leaves_no_source_or_receipt(true).await;
}
#[tokio::test]
#[expect(
    clippy::large_futures,
    reason = "Keep the conformance helper's original await and allocation shape."
)]
async fn surreal_stale_contact_admission_is_atomic() {
    stale_admission_leaves_no_source_or_receipt(false).await;
}

async fn guard_with_preceding_effect(
    storage: &StorageClient,
    state: &AcceptedContactState,
    marker: &Entity,
) -> StorageResult<()> {
    if let Some(store) = storage.embedded_store() {
        store
            .mutate(|tx| {
                tx.create(marker.clone())?;
                state.check_tx(tx)
            })
            .await
    } else {
        let mut sql = String::from("BEGIN TRANSACTION; CREATE $marker_id CONTENT $marker;\n");
        let mut bindings = crate::bindings! {"marker_id"=>crate::surreal::thing_from_record_id(&marker.base.id),"marker"=>CanonicalRecord::Entity(Box::new(marker.clone())).native()?};
        state.append_guard_sql(&mut sql, &mut bindings)?;
        sql.push_str("COMMIT TRANSACTION;");
        storage.query_bind_checked(&sql, bindings).await?;
        Ok(())
    }
}
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture, repository, and mutation owners alive through the original conformance phase."
)]
async fn sealed_predicates_abort_the_actual_transaction(embedded: bool) {
    let storage = client(embedded).await;
    let repo = SourcePublicationRepository::new(storage.clone());
    let request = request();
    let contact = PreparedSourceContact::prepare(&storage, &request.source)
        .await
        .unwrap()
        .unwrap();
    let receipt = repo
        .accept_with_prepared_contact(request, &contact)
        .await
        .unwrap();
    let fence = publication(&receipt);
    let canonical = CanonicalRepository::new(storage.clone());
    let state = AcceptedContactState::load(&storage, "contact-space", &fence)
        .await
        .unwrap();
    let mut plan = canonical
        .prepare_source_replacement("contact-space", &fence)
        .await
        .unwrap();
    assert!(
        state.validate_plan(&plan).is_err(),
        "cannot complete by omitting accepted contact effects"
    );
    canonical
        .append_accepted_source_effects(&mut plan, "contact-space", &fence)
        .await
        .unwrap();
    state.validate_plan(&plan).unwrap();
    let first = plan.effects.remove(0);
    assert!(state.validate_plan(&plan).is_err());
    plan.effects.insert(0, first);
    let marker = Entity::new(
        base(TableName::Entity),
        EntityType::new("project"),
        "guard positive".into(),
    );
    guard_with_preceding_effect(&storage, &state, &marker)
        .await
        .unwrap();
    company(&storage).await;
    let marker = Entity::new(
        base(TableName::Entity),
        EntityType::new("project"),
        "must roll back".into(),
    );
    assert!(
        guard_with_preceding_effect(&storage, &state, &marker)
            .await
            .is_err(),
        "original absence predicate must fail without a global epoch check"
    );
    assert!(crate::EntityRepository::new(storage.clone())
        .get("contact-space", &marker.base.id)
        .await
        .unwrap()
        .is_none());
    assert_eq!(
        repo.operation("contact-space", &receipt.operation_id)
            .await
            .unwrap()
            .unwrap()
            .state,
        OperationState::Accepted
    );
    assert_eq!(
        repo.head("contact-space", &receipt.source_id)
            .await
            .unwrap()
            .unwrap()
            .published_revision,
        None
    );
}
#[tokio::test]
#[expect(
    clippy::large_futures,
    reason = "Keep the conformance helper's original await and allocation shape."
)]
async fn embedded_contact_predicate_fault_rolls_back_preceding_effect() {
    sealed_predicates_abort_the_actual_transaction(true).await;
}
#[tokio::test]
#[expect(
    clippy::large_futures,
    reason = "Keep the conformance helper's original await and allocation shape."
)]
async fn surreal_contact_predicate_fault_rolls_back_preceding_effect() {
    sealed_predicates_abort_the_actual_transaction(false).await;
}

#[test]
fn erasure_recreation_preview_has_stable_fresh_intent_identity() {
    let old = RecordId::new_v4(TableName::Source);
    let first =
        crate::source_publication::recreated_source_id("contact-space", &old, "intent-1").unwrap();
    assert_ne!(first, old);
    assert_eq!(
        first,
        crate::source_publication::recreated_source_id("contact-space", &old, "intent-1").unwrap()
    );
    assert_ne!(
        first,
        crate::source_publication::recreated_source_id("contact-space", &old, "intent-2").unwrap()
    );
    assert_ne!(
        first,
        crate::source_publication::recreated_source_id("other-space", &old, "intent-1").unwrap()
    );
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture, repository, and mutation owners alive through the original conformance phase."
)]
async fn erased_payload_is_not_a_contact_retry(embedded: bool) {
    let storage = client(embedded).await;
    let repo = SourcePublicationRepository::new(storage.clone());
    let request = request();
    let contact = PreparedSourceContact::prepare(&storage, &request.source)
        .await
        .unwrap()
        .unwrap();
    let receipt = repo
        .accept_with_prepared_contact(request.clone(), &contact)
        .await
        .unwrap();
    let mut head = repo
        .head("contact-space", &receipt.source_id)
        .await
        .unwrap()
        .unwrap();
    // Isolated authority test: the real physical erase/replay oracle is owned by E.
    head.lifecycle = SourceVisibility::Erased;
    head.updated_at = Utc::now();
    let key = crate::source_publication::head_key("contact-space", &receipt.source_id);
    if let Some(store) = storage.embedded_store() {
        store
            .mutate(|tx| {
                write_control_tx(
                    tx,
                    crate::source_publication::HEAD_TABLE,
                    &key,
                    "contact-space",
                    &receipt.source_id,
                    &head,
                )
            })
            .await
            .unwrap();
    } else {
        storage.query_bind_checked("UPDATE $id CONTENT $head",crate::bindings! {"id"=>control_thing(crate::source_publication::HEAD_TABLE,&key),"head"=>control_value("contact-space",&receipt.source_id,&head).unwrap()}).await.unwrap();
    }
    assert!(
        repo.prepared_contact_retry(&request).await.is_err(),
        "even a residual attachment must not leak erased payload"
    );
    let mut recreate = request;
    recreate.idempotency_key = "new-recreation".into();
    recreate.recreate_after_erasure = true;
    let preview = repo.acceptance_source(&recreate).await.unwrap();
    assert_ne!(preview.base.id, receipt.source_id);
    assert_eq!(
        preview.base.id,
        repo.acceptance_source(&recreate).await.unwrap().base.id
    );
}
#[tokio::test]
#[expect(
    clippy::large_futures,
    reason = "Keep the conformance helper's original await and allocation shape."
)]
async fn embedded_erased_contact_authority_cannot_be_replayed() {
    erased_payload_is_not_a_contact_retry(true).await;
}
#[tokio::test]
#[expect(
    clippy::large_futures,
    reason = "Keep the conformance helper's original await and allocation shape."
)]
async fn surreal_erased_contact_authority_cannot_be_replayed() {
    erased_payload_is_not_a_contact_retry(false).await;
}

async fn retract_request(
    repo: &SourcePublicationRepository,
    head: &SourceHead,
    key: &str,
) -> crate::source_publication::SourceMutation {
    crate::source_publication::SourceMutation {
        space_id: head.space_id.clone(),
        source_id: head.source_id.clone(),
        expected: SourcePrecondition::Generation(head.generation.clone()),
        idempotency_key: key.into(),
        request_fingerprint: key.into(),
        actor: "test".into(),
        authorized: repo
            .authorization_snapshot(&head.space_id, &head.source_id)
            .await
            .unwrap()
            .unwrap(),
        authorized_document: None,
    }
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture, repository, and mutation owners alive through the original conformance phase."
)]
async fn restore_uses_exact_pending_acceptance_through_repeated_retraction(embedded: bool) {
    let storage = client(embedded).await;
    let repo = SourcePublicationRepository::new(storage.clone());
    let original_request = request();
    let contact = PreparedSourceContact::prepare(&storage, &original_request.source)
        .await
        .unwrap()
        .unwrap();
    let accepted = repo
        .accept_with_prepared_contact(original_request.clone(), &contact)
        .await
        .unwrap();
    assert_eq!(
        accepted.published_revision, None,
        "predecessor is acceptance, not a fabricated completed publication"
    );
    let original = publication(&accepted);
    let head = repo
        .head("contact-space", &original.source_id)
        .await
        .unwrap()
        .unwrap();
    let retract = retract_request(&repo, &head, "first-retraction").await;
    repo.retract(retract).await.unwrap();
    let retracted = repo
        .head("contact-space", &original.source_id)
        .await
        .unwrap()
        .unwrap();
    assert_ne!(retracted.generation, original.generation);
    assert_eq!(
        repo.publication_before_retraction("contact-space", &retracted)
            .await
            .unwrap(),
        original
    );
    repo.retract(retract_request(&repo, &retracted, "repeated-retraction").await)
        .await
        .unwrap();
    let repeated = repo
        .head("contact-space", &original.source_id)
        .await
        .unwrap()
        .unwrap();
    assert!(
        repo.publication_before_retraction("contact-space", &retracted)
            .await
            .is_err(),
        "a replaced lifecycle observation cannot be refreshed implicitly"
    );
    assert_eq!(
        repo.publication_before_retraction("contact-space", &repeated)
            .await
            .unwrap(),
        original
    );
    let mut restore = original_request.clone();
    restore.revision_id = "explicit-restored-contact".into();
    restore.idempotency_key = "explicit-restore".into();
    restore.request_fingerprint = "explicit-restore-input".into();
    restore.expected = SourcePrecondition::Generation(repeated.generation.clone());
    restore.authorized = repo
        .authorization_snapshot("contact-space", &original.source_id)
        .await
        .unwrap();
    restore.reactivate = true;
    let fresh = repo
        .prepared_restore_contact(&restore, &repeated)
        .await
        .unwrap()
        .unwrap();
    let restored = repo
        .accept_with_prepared_contact(restore.clone(), &fresh)
        .await
        .unwrap();
    assert_ne!(restored.source_generation, repeated.generation);
    company(&storage).await;
    company(&storage).await;
    assert_eq!(repo.prepared_restore_contact(&restore, &repeated).await.unwrap().unwrap().digest(), fresh.digest(), "matching restore retry returns original capability before checking old retracted head/selectors");
    let mut new_intent = restore;
    new_intent.idempotency_key = "unaccepted-restore".into();
    assert!(repo
        .prepared_restore_contact(&new_intent, &repeated)
        .await
        .is_err());
}
#[tokio::test]
#[expect(
    clippy::large_futures,
    reason = "Keep the conformance helper's original await and allocation shape."
)]
async fn embedded_restore_reads_original_acceptance_through_retraction() {
    restore_uses_exact_pending_acceptance_through_repeated_retraction(true).await;
}
#[tokio::test]
#[expect(
    clippy::large_futures,
    reason = "Keep the conformance helper's original await and allocation shape."
)]
async fn surreal_restore_reads_original_acceptance_through_retraction() {
    restore_uses_exact_pending_acceptance_through_repeated_retraction(false).await;
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture, repository, and mutation owners alive through the original conformance phase."
)]
#[expect(
    clippy::too_many_lines,
    reason = "Keep the complete fault and recovery sequence together with its original assertions."
)]
async fn missing_original_plan_keeps_only_original_admission(embedded: bool) {
    let storage = client(embedded).await;
    let repo = SourcePublicationRepository::new(storage.clone());
    let request = request();
    let original = PreparedSourceContact::prepare(&storage, &request.source)
        .await
        .unwrap()
        .unwrap();
    let receipt = repo
        .accept_with_prepared_contact(request.clone(), &original)
        .await
        .unwrap();
    let fence = publication(&receipt);
    // Deliberate payload-loss fault at the real private storage boundary. Full
    // multi-source physical erasure is a separate F/E runtime oracle.
    let key = contact_key("contact-space", &fence);
    if let Some(store) = storage.embedded_store() {
        store
            .mutate(|tx| {
                tx.hard_delete_extension_row(
                    CONTACT_TABLE,
                    &format!("{CONTACT_TABLE}:{key}"),
                    "contact-space",
                )
                .map_err(Into::into)
            })
            .await
            .unwrap();
    } else {
        storage
            .query_bind_checked(
                "DELETE $row;",
                crate::bindings! {"row"=>control_thing(CONTACT_TABLE,&key)},
            )
            .await
            .unwrap();
    }
    let capability = repo
        .accepted_contact_capability_for_publication("contact-space", &fence)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(capability.digest(), original.digest());
    assert_eq!(capability.publication(), &fence);
    assert!(
        matches!(repo.source_acceptance_retry(&request).await.unwrap(),SourceAcceptanceRetry::ExistingContactPlanUnavailable {operation,capability} if operation==receipt && capability.publication()==&fence)
    );
    assert!(
        repo.prepared_contact_retry(&request).await.is_err(),
        "the erased plan must never be reconstructed for its original key"
    );
    assert!(repo
        .accepted_contact_for_publication("contact-space", &fence)
        .await
        .is_err());
    assert_eq!(
        repo.matching_source_receipt(&request).await.unwrap(),
        Some(receipt.clone())
    );
    let mut fresh = request.clone();
    fresh.revision_id = "fresh-after-plan-loss".into();
    fresh.idempotency_key = "fresh-after-plan-loss".into();
    fresh.request_fingerprint = "fresh-after-plan-loss".into();
    fresh.expected = SourcePrecondition::Generation(receipt.source_generation);
    fresh.authorized = repo
        .authorization_snapshot("contact-space", &fence.source_id)
        .await
        .unwrap();
    let contact = repo
        .prepared_reprocess_contact(&fresh, &fence)
        .await
        .unwrap()
        .unwrap();
    let accepted = repo
        .accept_with_prepared_contact(fresh.clone(), &contact)
        .await
        .unwrap();
    assert_ne!(accepted.source_generation, fence.generation);
    let current = publication(&accepted);
    let raw = repo
        .read_control_payload(OPERATION_TABLE, &current.operation_id, "contact-space")
        .await
        .unwrap()
        .unwrap();
    let mut old: serde_json::Value = crate::source_publication::decode_payload(&raw).unwrap();
    old.as_object_mut().unwrap().remove("contact_digest");
    if let Some(store) = storage.embedded_store() {
        store
            .mutate(|tx| {
                write_control_tx(
                    tx,
                    OPERATION_TABLE,
                    &current.operation_id,
                    "contact-space",
                    &current.source_id,
                    &old,
                )
            })
            .await
            .unwrap();
    } else {
        storage.query_bind_checked("UPDATE $row CONTENT $record;",crate::bindings!{"row"=>control_thing(OPERATION_TABLE,&current.operation_id),"record"=>control_value("contact-space",&current.source_id,&old).unwrap()}).await.unwrap();
    }
    assert!(
        matches!(repo.accepted_contact_capability_for_publication("contact-space",&current).await,Err(crate::StorageError::Conflict(code)) if code=="contact_admission_unknown")
    );
}
#[tokio::test]
#[expect(
    clippy::large_futures,
    reason = "Keep the conformance helper's original await and allocation shape."
)]
async fn embedded_contact_plan_loss_never_reconstructs_old_authority() {
    missing_original_plan_keeps_only_original_admission(true).await;
}
#[tokio::test]
#[expect(
    clippy::large_futures,
    reason = "Keep the conformance helper's original await and allocation shape."
)]
async fn surreal_contact_plan_loss_never_reconstructs_old_authority() {
    missing_original_plan_keeps_only_original_admission(false).await;
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture, repository, and mutation owners alive through the original conformance phase."
)]
async fn same_revision_bookkeeping_prepares_the_retained_immutable_source(embedded: bool) {
    let client = client(embedded).await;
    let repo = SourcePublicationRepository::new(client.clone());
    let mut first = request();
    first.source.metadata["observed_at"] = serde_json::json!("original observation");
    let contact = PreparedSourceContact::prepare(&client, &first.source)
        .await
        .unwrap()
        .unwrap();
    let receipt = repo
        .accept_with_prepared_contact(first.clone(), &contact)
        .await
        .unwrap();
    let original = repo
        .revision("contact-space", &receipt.source_id, &first.revision_id)
        .await
        .unwrap()
        .unwrap();
    let mut next = first.clone();
    next.expected = SourcePrecondition::Generation(receipt.source_generation.clone());
    next.idempotency_key = "fresh-intent-same-revision-new-observation".into();
    next.request_fingerprint = "explicit-fresh-bookkeeping-intent".into();
    next.authorized = repo
        .authorization_snapshot("contact-space", &receipt.source_id)
        .await
        .unwrap();
    next.source.metadata["observed_at"] = serde_json::json!("later observation");
    next.source.base.created_by = "another-intake-observer".into();
    // Direct callers cannot publish a seal for a different retained snapshot.
    let wrong = PreparedSourceContact::prepare(&client, &next.source)
        .await
        .unwrap()
        .unwrap();
    assert!(repo
        .accept_with_prepared_contact(next.clone(), &wrong)
        .await
        .is_err());
    assert!(repo.matching_source_receipt(&next).await.unwrap().is_none());
    let proposed = repo.acceptance_source(&next).await.unwrap();
    assert_eq!(proposed, original.source);
    next.source = proposed;
    let correct = PreparedSourceContact::prepare(&client, &next.source)
        .await
        .unwrap()
        .unwrap();
    let accepted = repo
        .accept_with_prepared_contact(next.clone(), &correct)
        .await
        .unwrap();
    assert_ne!(accepted.operation_id, receipt.operation_id);
    assert_ne!(accepted.source_generation, receipt.source_generation);
    assert_eq!(accepted.state, OperationState::Accepted);
    assert_eq!(
        repo.revision("contact-space", &accepted.source_id, &next.revision_id)
            .await
            .unwrap()
            .unwrap()
            .source,
        original.source
    );
    let original_intent =
        AcceptedContactState::load(&client, "contact-space", &publication(&accepted))
            .await
            .unwrap();
    original_intent
        .intent
        .unwrap()
        .contact
        .validate_source(&original.source)
        .unwrap();
    // Changed semantic metadata still cannot reuse an immutable revision.
    next.idempotency_key = "different-contact-under-same-revision".into();
    next.source.metadata["contact"]["name"] = serde_json::json!("Changed claim");
    assert!(repo.acceptance_source(&next).await.is_err());
}
#[tokio::test]
#[expect(
    clippy::large_futures,
    reason = "Keep the conformance helper's original await and allocation shape."
)]
async fn embedded_same_revision_contact_seals_original_immutable_input() {
    same_revision_bookkeeping_prepares_the_retained_immutable_source(true).await;
}
#[tokio::test]
#[expect(
    clippy::large_futures,
    reason = "Keep the conformance helper's original await and allocation shape."
)]
async fn surreal_same_revision_contact_seals_original_immutable_input() {
    same_revision_bookkeeping_prepares_the_retained_immutable_source(false).await;
}
