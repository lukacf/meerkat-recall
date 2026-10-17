#![allow(clippy::unwrap_used, clippy::too_many_lines)]

use super::{GetProvenanceHandler, ToolContext, ToolHandler};
use chrono::Utc;
use elephant_policy::{Clearance, Principal};
use elephant_storage::{
    AssertionRepository, AttributeRepository, DocNodeRepository, DocRepository,
    DocRevisionRepository, RelRepository, Repository, SourceRepository, StorageClient,
};
use elephant_types::records::assertion::EvidenceSpan;
use elephant_types::records::attribute::Literal;
use elephant_types::records::base::{BaseFields, SubjectsState};
use elephant_types::records::doc_node::{NodeRole, TextSpan};
use elephant_types::records::doc_revision::{RendererMetadata, RevisionStatus};
use elephant_types::records::rel::{ClaimStatus, EvidenceRef, TimeBasis};
use elephant_types::records::source::SourceType;
use elephant_types::records::{Assertion, Attribute, Doc, DocNode, DocRevision, Rel, Source};
use elephant_types::{Link, RecordId, TableName};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::collections::HashSet;

fn base(table: TableName, protected: bool) -> BaseFields {
    let mut fields = BaseFields::new(RecordId::new_v4(table), "alpha".into(), "fixture".into());
    fields.subjects_state = SubjectsState::Complete;
    fields.security.level = if protected { 8 } else { 0 };
    fields
}

async fn fixture(case: &str) -> (ToolContext, RecordId) {
    fixture_on(case, true).await
}

#[expect(
    clippy::bool_to_int_with_if,
    reason = "Keep the original fixture, ownership, and assertion scope intact."
)]
async fn fixture_on(case: &str, embedded: bool) -> (ToolContext, RecordId) {
    let storage = if embedded {
        StorageClient::new_embedded().await.unwrap()
    } else {
        StorageClient::new_memory().await.unwrap()
    };
    crate::retrieval_test_support::initialize_source_support(&storage)
        .await
        .unwrap();
    let mut source = Source::new(
        base(TableName::Source, case == "source"),
        SourceType::Manual,
        "fixture".into(),
    );
    if case == "deleted_source" {
        source.base.deleted_at = Some(Utc::now());
    }
    if case == "foreign_source" {
        source.base.space_id = "other".into();
    }
    if case != "missing_source" {
        SourceRepository::new(storage.clone())
            .create(source.clone())
            .await
            .unwrap();
    }
    let mut doc = Doc {
        base: base(TableName::Doc, case == "doc"),
        title: "Fixture document".into(),
        text: "évidence".into(),
        sidecar_text: None,
        source: (case != "source_less").then(|| Link::new(source.base.id.clone())),
        media_attachments: vec![],
        rendition_key: "fixture".into(),
        metadata: json!({}),
    };
    if case == "foreign_doc" {
        doc.base.space_id = "other".into();
    }
    if case == "deleted_doc" {
        doc.base.deleted_at = Some(Utc::now());
    }
    DocRepository::new(storage.clone())
        .create(doc.clone())
        .await
        .unwrap();
    let revision = DocRevision {
        base: base(TableName::DocRevision, case == "revision"),
        doc: Link::new(doc.base.id.clone()),
        text: doc.text,
        text_hash: format!(
            "sha256:{}",
            hex::encode(Sha256::digest("évidence".as_bytes()))
        ),
        renderer: RendererMetadata {
            name: "fixture".into(),
            version: "1".into(),
            config_hash: None,
        },
        manifest: None,
        supersedes: None,
        status: RevisionStatus::Active,
        rendered_at: Utc::now(),
    };
    DocRevisionRepository::new(storage.clone())
        .create(revision.clone())
        .await
        .unwrap();
    let mut node = DocNode {
        base: base(TableName::DocNode, case == "node"),
        revision: Link::new(revision.base.id.clone()),
        role: NodeRole::Paragraph,
        span: TextSpan { start: 0, end: 9 },
        node_key: "fixture".into(),
        text: Some("évidence".into()),
        parent: None,
        position: 0,
        depth: 0,
        embedding_model: None,
        collection: None,
    };
    if case == "mixed_node_parent" {
        node.revision = Link::new(RecordId::new_v4(TableName::DocRevision));
    }
    if case != "missing_node" {
        DocNodeRepository::new(storage.clone())
            .create(node.clone())
            .await
            .unwrap();
    }
    let span = EvidenceSpan {
        revision: Link::new(revision.base.id.clone()),
        node: Some(Link::new(node.base.id.clone())),
        start: if case == "bad_span" { 1 } else { 0 },
        end: if case == "empty_span" {
            0
        } else if case == "outside_span" {
            99
        } else {
            9
        },
        quote_hash: if case == "bad_hash" {
            "synthetic".into()
        } else {
            format!(
                "sha256:{}",
                hex::encode(Sha256::digest("évidence".as_bytes()))
            )
        },
    };
    let mut assertion_value =
        serde_json::to_value(base(TableName::Assertion, case == "assertion")).unwrap();
    assertion_value.as_object_mut().unwrap().extend(json!({
        "subject_text":"fixture subject", "predicate_raw":"related_to", "evidence":span,
        "observed_at":Utc::now(), "asserted_at":Utc::now(), "assertion_key":"fixture", "confidence":0.8,
    }).as_object().unwrap().clone());
    let mut assertion: Assertion = serde_json::from_value(assertion_value).unwrap();
    if !matches!(case, "source_less" | "missing_assertion_lineage") {
        assertion.base.provenance.sources = vec![if case == "mismatched_assertion_lineage" {
            RecordId::new_v4(TableName::Source)
        } else {
            source.base.id.clone()
        }];
    }
    if case == "mixed_assertion_parent" {
        assertion.evidence.revision = Link::new(RecordId::new_v4(TableName::DocRevision));
    }
    if case == "mismatched_span" {
        assertion.evidence.start = 2;
    }
    if case == "mismatched_hash" {
        assertion.evidence.quote_hash = "different".into();
    }
    if case == "mismatched_node" {
        assertion.evidence.node = None;
    }
    AssertionRepository::new(storage.clone())
        .create(assertion.clone())
        .await
        .unwrap();
    let evidence = vec![EvidenceRef {
        assertion: Link::new(assertion.base.id),
        span,
        time_basis: TimeBasis::ObservedAt,
        time_value: Utc::now(),
    }];
    let id = if case.starts_with("attribute") {
        let attribute = Attribute {
            base: base(TableName::Attribute, case == "attribute_protected"),
            subject: Link::new(RecordId::new_v4(TableName::Entity)),
            attr_type: "fixture".into(),
            value: Literal::String("private attribute".into()),
            status: ClaimStatus::Candidate,
            confidence: 0.8,
            validity_window: None,
            evidence,
            temporal_audit: None,
        };
        let id = attribute.base.id.clone();
        AttributeRepository::new(storage.clone())
            .create(attribute)
            .await
            .unwrap();
        id
    } else {
        let rel = Rel {
            base: base(TableName::Rel, case == "claim"),
            subject: Link::new(RecordId::new_v4(TableName::Entity)),
            object: Link::new(RecordId::new_v4(TableName::Entity)),
            rel_type: "related_to".into(),
            status: ClaimStatus::Candidate,
            confidence: 0.8,
            validity_window: None,
            evidence,
            temporal_audit: None,
        };
        let id = rel.base.id.clone();
        RelRepository::new(storage.clone())
            .create(rel)
            .await
            .unwrap();
        id
    };
    let principal = Principal {
        space_id: "alpha".into(),
        scopes: HashSet::from(["*".into()]),
        clearances: vec![Clearance {
            policy_id: "default".into(),
            max_level: 5,
            labels: HashSet::new(),
            handling_allow: HashSet::new(),
        }],
        subject_allowlist: None,
        purpose: HashSet::new(),
    };
    (ToolContext::new(storage, principal), id)
}

#[tokio::test]
async fn provenance_authorizes_each_record_before_returning_the_chain() {
    for case in [
        "claim",
        "assertion",
        "revision",
        "node",
        "doc",
        "foreign_doc",
        "deleted_doc",
        "missing_node",
        "attribute_protected",
        "mixed_node_parent",
        "mixed_assertion_parent",
        "source",
        "missing_source",
        "deleted_source",
        "foreign_source",
        "bad_span",
        "bad_hash",
        "empty_span",
        "outside_span",
        "mismatched_span",
        "mismatched_hash",
        "mismatched_node",
        "missing_assertion_lineage",
        "mismatched_assertion_lineage",
    ] {
        let (ctx, id) = fixture(case).await;
        let result = GetProvenanceHandler::new(ctx.storage.clone())
            .execute(&ctx, json!({"id":id.to_string()}))
            .await;
        assert!(
            result.is_err(),
            "protected or missing {case} cannot yield evidence"
        );
    }
}

#[tokio::test]
async fn provenance_returns_authorized_rel_and_attribute_evidence() {
    for (case, embedded) in [true, false].into_iter().flat_map(|embedded| {
        ["allowed", "attribute_allowed", "source_less"].map(|case| (case, embedded))
    }) {
        let (ctx, id) = fixture_on(case, embedded).await;
        let result = GetProvenanceHandler::new(ctx.storage.clone())
            .execute(&ctx, json!({"id":id.to_string()}))
            .await
            .unwrap();
        assert_eq!(result["ok"], true);
        assert_eq!(result["data"]["chain"][0]["quote"], "évidence");
        assert_eq!(result["data"]["evidence_count"], 1);
    }
}

#[tokio::test]
async fn provenance_does_not_slice_inside_a_utf8_character() {
    let (ctx, id) = fixture("bad_span").await;
    let result = GetProvenanceHandler::new(ctx.storage.clone())
        .execute(&ctx, json!({"id":id.to_string()}))
        .await;
    assert!(
        result.is_err(),
        "invalid UTF-8 boundaries must fail, not yield a partial chain"
    );
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep the original fixture, ownership, and assertion scope intact."
)]
async fn current_source_deletion_fences_actual_raw_reads_and_provenance_before_cleanup() {
    use crate::handlers::docs::{GetDocChunkHandler, GetDocHandler};
    for embedded in [false, true] {
        let (ctx, id) = fixture_on("allowed", embedded).await;
        let rel = RelRepository::new(ctx.storage.clone())
            .get("alpha", &id)
            .await
            .unwrap()
            .unwrap();
        let span = &rel.evidence[0].span;
        let revision = DocRevisionRepository::new(ctx.storage.clone())
            .get("alpha", span.revision.id())
            .await
            .unwrap()
            .unwrap();
        let doc = DocRepository::new(ctx.storage.clone())
            .get("alpha", revision.doc.id())
            .await
            .unwrap()
            .unwrap();
        let source = doc.source.as_ref().unwrap().id();
        let get_doc = GetDocHandler::new(ctx.storage.clone());
        let get_node = GetDocChunkHandler::new(ctx.storage.clone());
        assert_eq!(
            get_doc
                .execute(&ctx, json!({"id":doc.base.id.to_string()}))
                .await
                .unwrap()["ok"],
            true
        );
        let node = get_node
            .execute(
                &ctx,
                json!({"id":span.node.as_ref().unwrap().id().to_string()}),
            )
            .await
            .unwrap();
        assert_eq!(
            node["data"]["text"], "évidence",
            "unvalidated cached node text never escapes"
        );
        SourceRepository::new(ctx.storage.clone())
            .delete("alpha", source)
            .await
            .unwrap();
        assert!(
            DocRepository::new(ctx.storage.clone())
                .get("alpha", &doc.base.id)
                .await
                .unwrap()
                .is_some(),
            "derived payload still exists before cleanup"
        );
        assert!(get_doc
            .execute(&ctx, json!({"id":doc.base.id.to_string()}))
            .await
            .is_err());
        assert!(get_node
            .execute(
                &ctx,
                json!({"id":span.node.as_ref().unwrap().id().to_string()})
            )
            .await
            .is_err());
        assert!(GetProvenanceHandler::new(ctx.storage.clone())
            .execute(&ctx, json!({"id":id.to_string()}))
            .await
            .is_err());
    }
}

#[tokio::test]
async fn response_fence_rejects_source_change_after_an_early_handler_read() {
    use crate::handlers::docs::GetDocHandler;
    let (ctx, id) = fixture("allowed").await;
    let rel = RelRepository::new(ctx.storage.clone())
        .get("alpha", &id)
        .await
        .unwrap()
        .unwrap();
    let revision = DocRevisionRepository::new(ctx.storage.clone())
        .get("alpha", rel.evidence[0].span.revision.id())
        .await
        .unwrap()
        .unwrap();
    let doc = DocRepository::new(ctx.storage.clone())
        .get("alpha", revision.doc.id())
        .await
        .unwrap()
        .unwrap();
    let result = elephant_storage::read_eligibility::with_read_fence(ctx.storage.clone(), async {
        let early = GetDocHandler::new(ctx.storage.clone())
            .execute(&ctx, json!({"id":doc.base.id.to_string()}))
            .await
            .unwrap();
        SourceRepository::new(ctx.storage.clone())
            .delete("alpha", doc.source.as_ref().unwrap().id())
            .await
            .unwrap();
        early
    })
    .await;
    assert!(
        result.is_err(),
        "publication/source fence rejects previously assembled content"
    );
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep the original fixture, ownership, and assertion scope intact."
)]
async fn unsupported_derived_entities_are_hidden_but_source_less_authority_survives() {
    use crate::handlers::entities::GetEntityHandler;
    use elephant_storage::EntityRepository;
    use elephant_types::records::{Entity, EntityType};
    for embedded in [false, true] {
        let (ctx, _) = fixture_on("allowed", embedded).await;
        let mut derived = Entity::new(
            base(TableName::Entity, false),
            EntityType::new("person"),
            "unsupported content".into(),
        );
        derived
            .base
            .provenance
            .sources
            .push(RecordId::new_v4(TableName::Source));
        let independent = Entity::new(
            base(TableName::Entity, false),
            EntityType::new("person"),
            "independently authored".into(),
        );
        let repo = EntityRepository::new(ctx.storage.clone());
        repo.create(derived.clone()).await.unwrap();
        repo.create(independent.clone()).await.unwrap();
        let handler = GetEntityHandler::new(ctx.storage.clone());
        assert!(handler
            .execute(&ctx, json!({"id":derived.base.id.to_string()}))
            .await
            .is_err());
        let result = handler
            .execute(&ctx, json!({"id":independent.base.id.to_string()}))
            .await
            .unwrap();
        assert_eq!(result["data"]["display_name"], "independently authored");
    }
}
