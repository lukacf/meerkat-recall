#![allow(clippy::unwrap_used, clippy::expect_used)]
use super::*;
use chrono::Utc;
use elephant_storage::{DocRepository, DocRevisionRepository, SourceRepository};
use elephant_types::records::base::{BaseFields, ProvenanceStep};
use elephant_types::records::doc_node::{NodeRole, TextSpan};
use elephant_types::records::doc_revision::{RendererMetadata, RevisionStatus};
use elephant_types::records::entity::{Entity, EntityType};
use elephant_types::records::source::{Source, SourceType};
use elephant_types::records::{Doc, DocNode, DocRevision};
const SPACE: &str = "proposal-controls";
fn base(table: TableName) -> BaseFields {
    BaseFields::new(RecordId::new_v4(table), SPACE.into(), "original".into())
}
async fn fixture() -> (
    StorageClient,
    GroundingLineage,
    ActionDispositionProposal,
    elephant_types::records::Assertion,
) {
    let storage = StorageClient::new_embedded().await.unwrap();
    let mut config = serde_json::to_value(base(TableName::SpaceConfig)).unwrap();
    config.as_object_mut().unwrap().extend(serde_json::json!({"embedding_provider":"fake","embedding_model":"unused","embedding_dims":8}).as_object().unwrap().clone());
    elephant_storage::SpaceConfigRepository::new(storage.clone())
        .create(serde_json::from_value(config).unwrap())
        .await
        .unwrap();
    let source = Source::new(
        base(TableName::Source),
        SourceType::Manual,
        "original-source-hash".into(),
    );
    SourceRepository::new(storage.clone())
        .create(source.clone())
        .await
        .unwrap();
    let doc = Doc {
        base: base(TableName::Doc),
        title: "Owner statement".into(),
        text: "prefix: I am closing this action.".into(),
        sidecar_text: None,
        source: Some(Link::new(source.base.id.clone())),
        media_attachments: vec![],
        rendition_key: "original".into(),
        metadata: serde_json::json!({"recorded_by":"not-authority"}),
    };
    DocRepository::new(storage.clone())
        .create(doc.clone())
        .await
        .unwrap();
    let mut revision = DocRevision {
        base: base(TableName::DocRevision),
        doc: Link::new(doc.base.id.clone()),
        text: doc.text.clone(),
        text_hash: AssertionValidator::hash_quote(&doc.text),
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
    revision.base.provenance.chain.push(ProvenanceStep {
        step: super::super::grounding::SOURCE_CONTENT_STEP.into(),
        input: source.base.id.to_string(),
        output: source.hash.clone(),
    });
    DocRevisionRepository::new(storage.clone())
        .create(revision.clone())
        .await
        .unwrap();
    let node = DocNode {
        base: base(TableName::DocNode),
        revision: Link::new(revision.base.id.clone()),
        role: NodeRole::Chunk,
        span: TextSpan::new(0, doc.text.len() as u32),
        node_key: "original".into(),
        text: None,
        parent: None,
        position: 0,
        depth: 0,
        embedding_model: None,
        collection: None,
    };
    elephant_storage::DocNodeRepository::new(storage.clone())
        .create(node.clone())
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
    // The old request's own evidence is independent and remains unchanged.
    let old_doc = Doc {
        base: base(TableName::Doc),
        source: None,
        text: "Please complete the request.".into(),
        rendition_key: "original-request".into(),
        ..doc.clone()
    };
    DocRepository::new(storage.clone())
        .create(old_doc.clone())
        .await
        .unwrap();
    let old_revision = DocRevision {
        base: base(TableName::DocRevision),
        doc: Link::new(old_doc.base.id.clone()),
        text: old_doc.text.clone(),
        text_hash: AssertionValidator::hash_quote(&old_doc.text),
        ..revision.clone()
    };
    DocRevisionRepository::new(storage.clone())
        .create(old_revision.clone())
        .await
        .unwrap();
    let span = EvidenceSpan {
        revision: Link::new(old_revision.base.id.clone()),
        node: None,
        start: 0,
        end: old_revision.text.len() as u32,
        quote_hash: AssertionValidator::hash_quote(&old_revision.text),
    };
    let mut raw = serde_json::to_value(base(TableName::Assertion)).unwrap();
    raw.as_object_mut().unwrap().extend(serde_json::json!({"subject_text":"Owner","predicate_raw":"requires_action","object_text":"Original request","subject_entity":subject.base.id,"evidence":span,"asserted_at":Utc::now(),"observed_at":Utc::now(),"assertion_key":"original-request"}).as_object().unwrap().clone());
    let action: elephant_types::records::Assertion = serde_json::from_value(raw).unwrap();
    AssertionRepository::new(storage.clone())
        .create(action.clone())
        .await
        .unwrap();
    let proposal = ActionDispositionProposal {
        target_action_id: action.base.id.clone(),
        subject_entity_id: subject.base.id,
        reason: "Owner explicitly closes this request".into(),
        evidence_start: 0,
        evidence_end: (doc.text.len() - 8) as u32,
        evidence_quote: doc.text[8..].into(),
    };
    (
        storage,
        GroundingLineage {
            source,
            doc,
            revision,
            node,
        },
        proposal,
        action,
    )
}
#[tokio::test]
async fn action_proposal_links_exact_current_action_without_mutation_or_authority() {
    let (storage, lineage, proposal, action) = fixture().await;
    let canonical = CanonicalRepository::new(storage.clone());
    let epoch = canonical.observe_epoch(SPACE).await.unwrap();
    let result = prepare(
        &storage,
        &lineage,
        [8, lineage.revision.text.len()],
        std::slice::from_ref(&proposal),
    )
    .await
    .unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].owner_statement.start, 8);
    assert_eq!(
        result[0].owner_statement.quote_hash,
        AssertionValidator::hash_quote(&proposal.evidence_quote)
    );
    assert_eq!(result[0].authority, "untrusted_model_proposal");
    assert_eq!(canonical.observe_epoch(SPACE).await.unwrap(), epoch);
    assert_eq!(
        AssertionRepository::new(storage.clone())
            .get(SPACE, &action.base.id)
            .await
            .unwrap(),
        Some(action)
    );
    assert!(elephant_storage::AuditLogRepository::new(storage.clone())
        .list(SPACE, 100, None)
        .await
        .unwrap()
        .is_empty());
    assert!(ActionDispositionRepository::new(storage)
        .current(SPACE, &proposal.target_action_id, &|_| true)
        .await
        .unwrap()
        .is_none());
}
#[tokio::test]
async fn action_proposal_refuses_forged_quote_ambiguous_target_subject_and_policy() {
    let (storage, lineage, proposal, action) = fixture().await;
    let window = [8, lineage.revision.text.len()];
    let mut bad = proposal.clone();
    bad.evidence_quote = "Owner submitted it".into();
    assert!(prepare(&storage, &lineage, window, &[bad]).await.is_err());
    assert!(prepare(
        &storage,
        &lineage,
        window,
        &[proposal.clone(), proposal.clone()]
    )
    .await
    .is_err());
    let mut bad = proposal.clone();
    bad.subject_entity_id = RecordId::new_v4(TableName::Entity);
    assert!(prepare(&storage, &lineage, window, &[bad]).await.is_err());
    let mut bad = proposal.clone();
    bad.target_action_id = RecordId::new_v4(TableName::Assertion);
    assert!(prepare(&storage, &lineage, window, &[bad]).await.is_err());
    let mut changed = action.clone();
    changed.base.security.labels.push("restricted".into());
    AssertionRepository::new(storage.clone())
        .update(SPACE, &action.base.id, changed)
        .await
        .unwrap();
    assert!(prepare(&storage, &lineage, window, &[proposal])
        .await
        .is_err());
}
#[tokio::test]
async fn action_proposal_never_reopens_closed_action() {
    let (storage, lineage, proposal, action) = fixture().await;
    let repo = ActionDispositionRepository::new(storage.clone());
    // Authenticate separately against the old request evidence for this test;
    // proposal preparation itself cannot call this mutation.
    repo.close(
        SPACE,
        repo.begin(SPACE).await.unwrap(),
        "authenticated-owner",
        elephant_storage::action_disposition::CloseActionInput {
            action_id: action.base.id.clone(),
            subject_entity: proposal.subject_entity_id.clone(),
            owner_statement: action.evidence.clone(),
            reason: "Explicit decision".into(),
            effective_at: Utc::now(),
        },
        &|_| true,
    )
    .await
    .unwrap();
    assert!(prepare(
        &storage,
        &lineage,
        [8, lineage.revision.text.len()],
        &[proposal]
    )
    .await
    .is_err());
}
#[tokio::test]
async fn action_proposal_utf8_and_selected_window_are_exact() {
    let (_, mut lineage, mut proposal, _) = fixture().await;
    lineage.revision.text = "é close".into();
    lineage.node.span = TextSpan::new(0, 8);
    proposal.evidence_start = 1;
    proposal.evidence_end = 2;
    proposal.evidence_quote = "é".into();
    assert!(evidence(&proposal, &lineage, [0, 8]).is_err());
    proposal.evidence_start = 0;
    proposal.evidence_end = 2;
    assert_eq!(evidence(&proposal, &lineage, [0, 8]).unwrap().end, 2);
    proposal.evidence_start = 0;
    proposal.evidence_end = 5;
    proposal.evidence_quote = "close".into();
    assert!(evidence(&proposal, &lineage, [3, 7]).is_err());
}
