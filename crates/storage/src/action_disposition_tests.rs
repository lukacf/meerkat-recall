//! Exact owner closure through the real canonical transaction, without providers.
#![allow(clippy::unwrap_used, clippy::expect_used)]
use super::*;
use crate::read_eligibility::CurrentRead;
use crate::{
    DocRepository, DocRevisionRepository, EntityRepository, EventRepository, SpaceConfigRepository,
};
use elephant_types::records::doc_revision::{RendererMetadata, RevisionStatus};
use elephant_types::records::entity::EntityType;
use elephant_types::records::{BaseFields, Doc, DocRevision, Entity, Event, SpaceConfig};
use elephant_types::Link;
use sha2::{Digest, Sha256};
const SPACE: &str = "action-test";

#[allow(
    clippy::too_many_lines,
    reason = "Keep the complete ordered fixture and its failure/recovery assertions together."
)]
async fn fixture(
    embedded: bool,
) -> (
    StorageClient,
    elephant_types::records::Assertion,
    Event,
    CloseActionInput,
) {
    let storage = if embedded {
        StorageClient::new_embedded().await.unwrap()
    } else {
        StorageClient::new_memory().await.unwrap()
    };
    if !embedded {
        storage
            .query_checked(concat!(
                include_str!("../../migrations/sql/031_source_publication.surql"),
                "\n",
                include_str!("../../migrations/sql/046_source_support_index_fence.surql")
            ))
            .await
            .unwrap();
        crate::reference_fence::install_native_reference_guards(&storage, &[])
            .await
            .unwrap();
    }
    let mut cfg = serde_json::to_value(base(TableName::SpaceConfig)).unwrap();
    cfg.as_object_mut().unwrap().extend(serde_json::json!({"embedding_provider":"fake","embedding_model":"unused","embedding_dims":8}).as_object().unwrap().clone());
    SpaceConfigRepository::new(storage.clone())
        .create(serde_json::from_value::<SpaceConfig>(cfg).unwrap())
        .await
        .unwrap();
    let subject = Entity::new(
        base(TableName::Entity),
        EntityType::new("person"),
        "Owner".into(),
    );
    EntityRepository::new(storage.clone())
        .create(subject.clone())
        .await
        .unwrap();
    let doc = Doc {
        base: base(TableName::Doc),
        title: "Original owner statement".into(),
        text: "I am closing this action; no further follow-up.".into(),
        sidecar_text: None,
        source: None,
        media_attachments: vec![],
        rendition_key: "owner".into(),
        metadata: serde_json::json!({"recorded_by":"identity:not-authority"}),
    };
    DocRepository::new(storage.clone())
        .create(doc.clone())
        .await
        .unwrap();
    let rev = DocRevision {
        base: base(TableName::DocRevision),
        doc: Link::new(doc.base.id.clone()),
        text: doc.text.clone(),
        text_hash: format!("{:x}", Sha256::digest(doc.text.as_bytes())),
        renderer: RendererMetadata {
            name: "original".into(),
            version: "1".into(),
            config_hash: None,
        },
        manifest: None,
        supersedes: None,
        status: RevisionStatus::Active,
        rendered_at: Utc::now(),
    };
    DocRevisionRepository::new(storage.clone())
        .create(rev.clone())
        .await
        .unwrap();
    let span = EvidenceSpan {
        revision: Link::new(rev.base.id.clone()),
        node: None,
        start: 0,
        end: u32::try_from(doc.text.len()).unwrap(),
        quote_hash: rev.text_hash.clone(),
    };
    let mut raw = serde_json::to_value(base(TableName::Assertion)).unwrap();
    raw.as_object_mut().unwrap().extend(serde_json::json!({"subject_text":"Owner","predicate_raw":"requires_action","object_text":"Original provider request","subject_entity":subject.base.id,"evidence":span,"asserted_at":Utc::now(),"observed_at":Utc::now(),"assertion_key":"original-request"}).as_object().unwrap().clone());
    let action: elephant_types::records::Assertion = serde_json::from_value(raw).unwrap();
    AssertionRepository::new(storage.clone())
        .create(action.clone())
        .await
        .unwrap();
    let event = Event::new(
        base(TableName::Event),
        "Original request timeline",
        vec![Link::new(action.base.id.clone())],
        vec![],
    )
    .unwrap();
    EventRepository::new(storage.clone())
        .create(event.clone())
        .await
        .unwrap();
    let input = CloseActionInput {
        action_id: action.base.id.clone(),
        subject_entity: subject.base.id,
        owner_statement: span,
        reason: "Closed by explicit owner disposition".into(),
        effective_at: Utc::now(),
    };
    (storage, action, event, input)
}
fn base(table: TableName) -> BaseFields {
    BaseFields::new(
        RecordId::new_v4(table),
        SPACE.into(),
        "original-author".into(),
    )
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Retain the same owner-action repository through replay, timeline projection and erased-parent refusal; the final assertion still uses it."
)]
async fn owner_closure_is_separate_exact_replay_and_timeline_projection_both_backends() {
    for embedded in [true, false] {
        let (storage, action, event, input) = fixture(embedded).await;
        let repository = ActionDispositionRepository::new(storage.clone());
        let result = Box::pin(repository.close(
            SPACE,
            repository.begin(SPACE).await.unwrap(),
            "authenticated-owner",
            input.clone(),
            &|_| true,
        ))
        .await
        .unwrap();
        assert_eq!(result.actor, "authenticated-owner");
        assert_eq!(result.state, "closed_by_owner");
        let epoch = repository.begin(SPACE).await.unwrap();
        let replay = Box::pin(repository.close(
            SPACE,
            epoch.clone(),
            "authenticated-owner",
            input.clone(),
            &|_| true,
        ))
        .await
        .unwrap();
        assert_eq!(result, replay);
        assert_eq!(repository.begin(SPACE).await.unwrap(), epoch);
        assert_eq!(
            AssertionRepository::new(storage.clone())
                .get(SPACE, &action.base.id)
                .await
                .unwrap(),
            Some(action.clone())
        );
        assert_eq!(
            EventRepository::new(storage.clone())
                .get(SPACE, &event.base.id)
                .await
                .unwrap(),
            Some(event.clone())
        );
        let mut projected = ReadEligibility::new(storage.clone())
            .project(event.clone(), &|_| true)
            .await
            .unwrap()
            .unwrap();
        projected
            .project_action_state(&ReadEligibility::new(storage.clone()), &|_| true)
            .await
            .unwrap();
        assert_eq!(projected.confidence.to_bits(), event.confidence.to_bits());
        assert_eq!(projected.status, event.status);
        assert_eq!(
            projected.metadata["recall_action_dispositions"][0]["state"],
            "closed_by_owner"
        );
        assert_eq!(
            projected.metadata["recall_action_dispositions"][0]["action_id"],
            action.base.id.to_string()
        );
        let mut changed = input.clone();
        changed.reason = "Different intent".into();
        assert!(Box::pin(repository.close(
            SPACE,
            epoch.clone(),
            "authenticated-owner",
            changed,
            &|_| true
        ))
        .await
        .is_err());
        assert!(repository
            .current(SPACE, &action.base.id, &|base| base.id
                != input.subject_entity)
            .await
            .unwrap()
            .is_none());
        assert_eq!(repository.begin(SPACE).await.unwrap(), epoch);
        // A foreign or erased parent can never leave a live closure projection.
        DocRevisionRepository::new(storage.clone())
            .delete(SPACE, input.owner_statement.revision.id())
            .await
            .unwrap();
        assert!(repository
            .current(SPACE, &action.base.id, &|_| true)
            .await
            .unwrap()
            .is_none());
    }
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Retain the same repository through stale authorization, joined concurrent intents and the final winner readback."
)]
async fn owner_closure_guards_stale_authorization_concurrent_intents_and_exact_target() {
    for embedded in [true, false] {
        let (storage, action, _, input) = fixture(embedded).await;
        let repository = ActionDispositionRepository::new(storage.clone());
        let epoch = repository.begin(SPACE).await.unwrap();
        EntityRepository::new(storage.clone())
            .create(Entity::new(
                base(TableName::Entity),
                EntityType::new("person"),
                "Concurrent edit".into(),
            ))
            .await
            .unwrap();
        assert!(
            Box::pin(repository.close(SPACE, epoch, "owner", input.clone(), &|_| true))
                .await
                .is_err()
        );
        let mut wrong = input.clone();
        wrong.subject_entity = RecordId::new_v4(TableName::Entity);
        assert!(Box::pin(repository.close(
            SPACE,
            repository.begin(SPACE).await.unwrap(),
            "owner",
            wrong,
            &|_| true
        ))
        .await
        .is_err());
        let mut forged = input.clone();
        forged.owner_statement.quote_hash = "0".repeat(64);
        assert!(Box::pin(repository.close(
            SPACE,
            repository.begin(SPACE).await.unwrap(),
            "owner",
            forged,
            &|_| true
        ))
        .await
        .is_err());
        let epoch = repository.begin(SPACE).await.unwrap();
        let mut other = input.clone();
        other.reason = "Another intent".into();
        let (first, second) = tokio::join!(
            repository.close(SPACE, epoch.clone(), "owner", input.clone(), &|_| true),
            repository.close(SPACE, epoch, "owner", other.clone(), &|_| true)
        );
        assert_ne!(first.is_ok(), second.is_ok());
        let winner = first.or(second).unwrap();
        assert_eq!(
            repository
                .current(SPACE, &action.base.id, &|_| true)
                .await
                .unwrap(),
            Some(winner)
        );
    }
}

#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "Keep the complete ordered fixture and its failure/recovery assertions together."
)]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Retain original document, action and canonical readers through source-head revocation and exact unchanged-row assertions."
)]
async fn owner_closure_exact_other_action_and_source_head_revocation_both_backends() {
    use crate::source_publication::{
        control_thing, control_value, head_key, write_control_tx, HEAD_TABLE,
    };
    use crate::{SourcePublicationRepository, SourceRepository};
    use elephant_types::publication::{SourceHead, SourceVisibility};
    use elephant_types::records::{Source, SourceType};
    for embedded in [true, false] {
        let (storage, action, event, input) = fixture(embedded).await;
        let mut other = action.clone();
        other.base.id = RecordId::new_v4(TableName::Assertion);
        other.assertion_key = "other-action-same-subject".into();
        AssertionRepository::new(storage.clone())
            .create(other.clone())
            .await
            .unwrap();
        let source = SourceRepository::new(storage.clone())
            .create(Source::new(
                base(TableName::Source),
                SourceType::Manual,
                "original-owner-source".into(),
            ))
            .await
            .unwrap();
        let revision = DocRevisionRepository::new(storage.clone())
            .get(SPACE, input.owner_statement.revision.id())
            .await
            .unwrap()
            .unwrap();
        let docs = DocRepository::new(storage.clone());
        let mut doc = docs.get(SPACE, revision.doc.id()).await.unwrap().unwrap();
        doc.source = Some(Link::new(source.base.id.clone()));
        docs.update(SPACE, revision.doc.id(), doc).await.unwrap();
        // This fixture now declares source-backed evidence; its original action
        // must carry the same source edge required by ordinary read eligibility.
        let mut action = action;
        action.base.provenance.sources = vec![source.base.id.clone()];
        let action = AssertionRepository::new(storage.clone())
            .update(SPACE, &action.base.id.clone(), action)
            .await
            .unwrap();
        let repository = ActionDispositionRepository::new(storage.clone());
        Box::pin(repository.close(
            SPACE,
            repository.begin(SPACE).await.unwrap(),
            "owner",
            input.clone(),
            &|_| true,
        ))
        .await
        .unwrap();
        assert!(repository
            .current(SPACE, &other.base.id, &|_| true)
            .await
            .unwrap()
            .is_none());
        let canonical = CanonicalRepository::new(storage.clone());
        let receipt = canonical
            .receipt_read_only(SPACE, &commit_id(SPACE, &action.base.id))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(receipt.source_ids, vec![source.base.id.clone()]);
        let action_before = AssertionRepository::new(storage.clone())
            .get(SPACE, &action.base.id)
            .await
            .unwrap();
        let doc_before = docs.get(SPACE, revision.doc.id()).await.unwrap();
        // Change only publication authority. Original claim/evidence rows stay
        // byte-identical, so fingerprint-only reads would incorrectly succeed.
        let head = SourceHead {
            space_id: SPACE.into(),
            source_id: source.base.id.clone(),
            generation: "revoked".into(),
            desired_revision: None,
            published_revision: None,
            operation_id: "fixture-retraction".into(),
            lifecycle: SourceVisibility::Retracted,
            updated_at: Utc::now(),
        };
        let key = head_key(SPACE, &source.base.id);
        if let Some(store) = storage.embedded_store() {
            store
                .mutate(|tx| write_control_tx(tx, HEAD_TABLE, &key, SPACE, &source.base.id, &head))
                .await
                .unwrap();
        } else {
            storage.query_bind_checked("UPSERT $id CONTENT $value;",crate::bindings!{"id"=>control_thing(HEAD_TABLE,&key),"value"=>control_value(SPACE,&source.base.id,&head).unwrap()}).await.unwrap();
        }
        assert_eq!(
            AssertionRepository::new(storage.clone())
                .get(SPACE, &action.base.id)
                .await
                .unwrap(),
            action_before
        );
        assert_eq!(
            docs.get(SPACE, revision.doc.id()).await.unwrap(),
            doc_before
        );
        assert!(matches!(
            repository.current(SPACE, &action.base.id, &|_| true).await,
            Ok(None) | Err(_)
        ));
        assert!(SourcePublicationRepository::new(storage.clone())
            .head(SPACE, &source.base.id)
            .await
            .unwrap()
            .is_some());
        assert_eq!(
            EventRepository::new(storage.clone())
                .get(SPACE, &event.base.id)
                .await
                .unwrap(),
            Some(event)
        );
    }
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Retain the original canonical and action repositories through receipt tampering and the final refusal."
)]
async fn owner_closure_receipt_plan_rejects_rewritten_reason_even_with_matching_input_hash() {
    use crate::source_publication::{control_thing, control_value, write_control_tx};
    for embedded in [true, false] {
        let (storage, action, _, input) = fixture(embedded).await;
        let repository = ActionDispositionRepository::new(storage.clone());
        Box::pin(repository.close(
            SPACE,
            repository.begin(SPACE).await.unwrap(),
            "owner",
            input.clone(),
            &|_| true,
        ))
        .await
        .unwrap();
        let canonical = CanonicalRepository::new(storage.clone());
        let mut receipt = canonical
            .receipt_read_only(SPACE, &commit_id(SPACE, &action.base.id))
            .await
            .unwrap()
            .unwrap();
        let mut result: ActionDisposition =
            serde_json::from_value(receipt.replay_output.clone().unwrap()).unwrap();
        let mut forged = input;
        forged.reason = "Fabricated close reason".into();
        result.reason = forged.reason.clone();
        result.request_fingerprint = request_fingerprint(SPACE, "owner", &forged).unwrap();
        receipt.input_fingerprint = result.request_fingerprint.clone();
        receipt.replay_output = Some(serde_json::to_value(result).unwrap());
        let key = stable_key(&[SPACE, &receipt.commit_id]);
        if let Some(store) = storage.embedded_store() {
            store
                .mutate(|tx| {
                    write_control_tx(
                        tx,
                        "source_commit_receipt",
                        &key,
                        SPACE,
                        &action.base.id,
                        &receipt,
                    )
                })
                .await
                .unwrap();
        } else {
            storage.query_bind_checked("UPSERT $id CONTENT $value;",crate::bindings!{"id"=>control_thing("source_commit_receipt",&key),"value"=>control_value(SPACE,&action.base.id,&receipt).unwrap()}).await.unwrap();
        }
        assert!(repository
            .current(SPACE, &action.base.id, &|_| true)
            .await
            .is_err());
    }
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Retain original action closure and each response reader through metadata preservation and stored-event readback assertions."
)]
async fn owner_closure_response_preserves_legacy_nonobject_event_metadata() {
    for embedded in [true, false] {
        let (storage, _, mut event, input) = fixture(embedded).await;
        let repository = ActionDispositionRepository::new(storage.clone());
        Box::pin(repository.close(
            SPACE,
            repository.begin(SPACE).await.unwrap(),
            "owner",
            input,
            &|_| true,
        ))
        .await
        .unwrap();
        for metadata in [
            serde_json::Value::Null,
            serde_json::json!(["original", 7]),
            serde_json::json!("original scalar"),
        ] {
            event.metadata = metadata.clone();
            event = EventRepository::new(storage.clone())
                .update(SPACE, &event.base.id, event.clone())
                .await
                .unwrap();
            let reader = ReadEligibility::new(storage.clone());
            let mut projected = reader
                .project(event.clone(), &|_| true)
                .await
                .unwrap()
                .unwrap();
            projected
                .project_action_state(&reader, &|_| true)
                .await
                .unwrap();
            reader.verify_current().await.unwrap();
            assert_eq!(projected.metadata["recall_original_metadata"], metadata);
            assert_eq!(
                projected.metadata["recall_action_dispositions"][0]["state"],
                "closed_by_owner"
            );
            assert_eq!(
                EventRepository::new(storage.clone())
                    .get(SPACE, &event.base.id)
                    .await
                    .unwrap(),
                Some(event.clone())
            );
        }
    }
}
