//! Query articles consume the prepared membership, never the whole subject set.
//! Retained-source fixtures exercise immutable evidence and publication on both
//! storage backends. The deterministic editorial stub is not a model oracle.
#![allow(clippy::expect_used)]

use super::*;
use elephant_storage::{DocNodeRepository, DocRepository, DocRevisionRepository, SourceRepository};
use elephant_types::records::doc_node::{NodeRole, TextSpan};
use elephant_types::records::doc_revision::RendererMetadata;
use elephant_types::records::{
    Assertion, AssertionRetryState, Doc, DocNode, DocRevision, EntityType, Event, RevisionStatus,
    Source, SourceType, SpaceConfig,
};

const QUOTE: &str = "Eve is Noah's daughter and an immediate member of Noah's family.";

fn base(table: TableName) -> BaseFields {
    BaseFields::new(
        RecordId::new_v4(table),
        "space".into(),
        "query-evidence-control".into(),
    )
}

async fn evidence(
    storage: &StorageClient,
    entity: &Entity,
    text: &str,
    security: Security,
) -> (Source, DocRevision, Assertion) {
    let hash = hex::encode(Sha256::digest(text.as_bytes()));
    let mut source_base = base(TableName::Source);
    source_base.security = security.clone();
    let source = SourceRepository::new(storage.clone())
        .create(Source::new(source_base, SourceType::Manual, hash.clone()))
        .await
        .expect("original retained source");
    let scoped = |table| {
        let mut value = base(table);
        value.security = security.clone();
        value.provenance.sources = vec![source.base.id.clone()];
        // Empty/Unknown is actual input authority, not a completeness claim.
        value.subjects_state = SubjectsState::Unknown;
        value
    };
    let doc = DocRepository::new(storage.clone())
        .create(Doc {
            base: scoped(TableName::Doc),
            title: "Original evidence".into(),
            text: text.into(),
            sidecar_text: None,
            source: Some(Link::new(source.base.id.clone())),
            media_attachments: vec![],
            rendition_key: Uuid::new_v4().to_string(),
            metadata: json!({}),
        })
        .await
        .expect("document");
    let revision = DocRevisionRepository::new(storage.clone())
        .create(DocRevision {
            base: scoped(TableName::DocRevision),
            doc: Link::new(doc.base.id),
            text: text.into(),
            text_hash: hash.clone(),
            renderer: RendererMetadata {
                name: "retained-test".into(),
                version: "1".into(),
                config_hash: None,
            },
            manifest: None,
            supersedes: None,
            status: RevisionStatus::Active,
            rendered_at: Utc::now(),
        })
        .await
        .expect("immutable revision");
    let end = u32::try_from(text.len()).expect("quote bytes");
    let node = DocNodeRepository::new(storage.clone())
        .create(DocNode {
            base: scoped(TableName::DocNode),
            revision: Link::new(revision.base.id.clone()),
            role: NodeRole::Paragraph,
            span: TextSpan::new(0, end),
            node_key: "original".into(),
            text: None,
            parent: None,
            position: 0,
            depth: 0,
            embedding_model: None,
            collection: None,
        })
        .await
        .expect("immutable node");
    let assertion = AssertionRepository::new(storage.clone())
        .create(Assertion {
            base: scoped(TableName::Assertion),
            subject_text: entity.display_name.clone(),
            predicate_raw: "recorded fact".into(),
            predicate_id: None,
            object_text: Some(text.into()),
            object_literal: None,
            evidence: EvidenceSpan {
                revision: Link::new(revision.base.id.clone()),
                node: Some(Link::new(node.base.id)),
                start: 0,
                end,
                quote_hash: hash,
            },
            source_event_time: None,
            observed_at: Utc::now(),
            asserted_at: Utc::now(),
            assertion_key: Uuid::new_v4().to_string(),
            resolution_status: ResolutionStatus::Resolved,
            resolution_method: None,
            resolution_confidence: None,
            retry_state: AssertionRetryState::default(),
            subject_entity: Some(Link::new(entity.base.id.clone())),
            object_entity: None,
            confidence: 1.0,
            subject_entity_type: None,
            object_entity_type: None,
            significance: None,
            lifecycle_tier: "active".into(),
        })
        .await
        .expect("resolved source assertion");
    (source, revision, assertion)
}

struct Fixture {
    storage: StorageClient,
    definition: KnowledgeViewDefinition,
    selected: Entity,
    unrelated: Entity,
    source: Source,
    revision: DocRevision,
    assertion: Assertion,
    event: Event,
}

impl Fixture {
    async fn new(embedded: bool) -> Self {
        let storage = if embedded {
            StorageClient::new_embedded()
                .await
                .expect("embedded storage")
        } else {
            let storage = StorageClient::new_memory()
                .await
                .expect("native memory storage");
            elephant_migrations::apply_schema(&storage)
                .await
                .expect("schema");
            storage
        };
        elephant_storage::SpaceConfigRepository::new(storage.clone())
            .create(SpaceConfig {
                base: base(TableName::SpaceConfig),
                embedding_provider: "unused".into(),
                embedding_model: "unused".into(),
                embedding_dims: 8,
                processing: None,
                purpose: None,
                skill: None,
            })
            .await
            .expect("named space");
        let repository = EntityRepository::new(storage.clone());
        let unrelated = repository
            .create(Entity::new(
                base(TableName::Entity),
                EntityType::new("person"),
                "Another person".into(),
            ))
            .await
            .expect("unselected entity");
        let mut selected = Entity::new(
            base(TableName::Entity),
            EntityType::new("person"),
            "Eve".into(),
        );
        // This subject contributes lineage but is not selected by the query.
        selected
            .base
            .subjects
            .push(Link::new(unrelated.base.id.clone()));
        let selected = repository.create(selected).await.expect("selected entity");
        let (source, revision, assertion) =
            evidence(&storage, &selected, QUOTE, selected.base.security.clone()).await;
        evidence(
            &storage,
            &unrelated,
            "A private unrelated event was recorded.",
            unrelated.base.security.clone(),
        )
        .await;
        let mut foreign = selected.base.security.clone();
        foreign.policy_id = "other-policy".into();
        evidence(
            &storage,
            &selected,
            "Foreign policy evidence must not enter the article.",
            foreign,
        )
        .await;
        let mut event_base = base(TableName::Event);
        event_base
            .subjects
            .push(Link::new(selected.base.id.clone()));
        event_base.provenance.sources.push(source.base.id.clone());
        let mut event = Event::new(
            event_base,
            "Family relationship recorded",
            vec![Link::new(assertion.base.id.clone())],
            vec![Link::new(source.base.id.clone())],
        )
        .expect("supported event");
        event.status = EventStatus::Approved;
        let event = EventRepository::new(storage.clone())
            .create(event)
            .await
            .expect("event");
        let mut definition_base = base(TableName::KnowledgeViewDefinition);
        definition_base.subjects_state = SubjectsState::Complete;
        let definition = KnowledgeViewDefinitionRepository::new(storage.clone())
            .upsert(KnowledgeViewDefinition {
                base: definition_base,
                title: "Eve".into(),
                publication_status: KnowledgeViewPublicationStatus::Published,
                related_articles: vec![],
                navigation: None,
                selector: KnowledgeViewSelector::Query {
                    entity_types: vec![],
                    name_contains: Some("Eve".into()),
                    attributes: vec![],
                    scan_limit: 16,
                    max_members: 4,
                },
            })
            .await
            .expect("published query definition");
        Self {
            storage,
            definition,
            selected,
            unrelated,
            source,
            revision,
            assertion,
            event,
        }
    }

    fn compiler(&self) -> WikiEditorialCompiler {
        WikiEditorialCompiler::new(
            self.storage.clone(),
            Arc::new(super::tests::MockEditorialLlm::default()),
            WikiEditorialConfig::default(),
        )
    }

    async fn context(&self) -> KnowledgeViewRevision {
        GraphViewCompiler::new(self.storage.clone())
            .prepare_context("space", &self.definition.base.id, false)
            .await
            .expect("actual bounded query context")
            .revision
    }
}

#[tokio::test]
async fn query_article_inherits_exact_selected_member_source_evidence() {
    for embedded in [true, false] {
        let fixture = Fixture::new(embedded).await;
        let mut context = fixture.context().await;
        let roots = article_context_roots(&fixture.definition, &context).expect("query roots");
        assert_eq!(roots, vec![Link::new(fixture.selected.base.id.clone())]);
        assert!(context
            .base
            .subjects
            .iter()
            .any(|subject| subject.id() == &fixture.unrelated.base.id));
        let compiler = fixture.compiler();
        compiler
            .augment_with_entity_source_evidence("space", &fixture.definition, &mut context)
            .await
            .expect("selected evidence");
        compiler
            .augment_with_root_semantic_records("space", &fixture.definition, &mut context)
            .await
            .expect("selected semantic records");
        let catalog = compiler
            .build_evidence_catalog("space", &mut context)
            .await
            .expect("exact catalog");
        let quotes = catalog
            .iter()
            .filter_map(|item| item.source_quote.as_deref())
            .collect::<Vec<_>>();
        assert_eq!(
            quotes,
            vec![QUOTE],
            "unselected or foreign evidence entered the article"
        );
        assert_eq!(context.base.subjects_state, SubjectsState::Unknown);
        assert_eq!(context.base.security, fixture.definition.base.security);
        assert!(context
            .dependencies
            .iter()
            .any(
                |dependency| dependency.record_id == fixture.event.base.id.to_string()
                    && dependency.role == "root_timeline"
            ));
        let source_citation = context
            .citations
            .iter()
            .find(|citation| citation.evidence_span.is_some())
            .expect("source span");
        assert_eq!(
            source_citation.evidence_span.as_ref(),
            Some(&fixture.assertion.evidence)
        );
        assert_eq!(
            source_citation.source_id,
            Some(fixture.source.base.id.to_string())
        );
        // Exercise the active compiler and canonical publication, not just the helper.
        let article = Box::pin(compiler.compile("space", &fixture.definition.base.id, true))
            .await
            .expect("source-backed query article");
        assert_eq!(article.view.kind, KnowledgeViewKind::Article);
        assert_eq!(article.view.base.subjects_state, SubjectsState::Unknown);
        assert_eq!(article.revision.base.subjects, article.view.base.subjects);
        assert!(article
            .revision
            .citations
            .iter()
            .any(|citation| citation.evidence_span.as_ref() == Some(&fixture.assertion.evidence)));
    }
}

#[tokio::test]
async fn query_evidence_roots_reject_foreign_or_unbounded_membership() {
    let fixture = Fixture::new(true).await;
    let context = fixture.context().await;
    let expected = article_context_roots(&fixture.definition, &context).expect("selected roots");
    let mut reversed = context.clone();
    reversed.dependencies.reverse();
    assert_eq!(
        article_context_roots(&fixture.definition, &reversed).expect("reordered"),
        expected
    );
    for mutation in ["root", "space", "type", "version", "subjects", "limit"] {
        let mut changed = context.clone();
        let mut definition = fixture.definition.clone();
        match mutation {
            "root" => {
                changed.root_record =
                    RecordId::new_v4(TableName::KnowledgeViewDefinition).to_string()
            }
            "space" => changed.base.space_id = "other-space".into(),
            "type" => {
                changed
                    .dependencies
                    .iter_mut()
                    .find(|d| d.role == "selected_member")
                    .expect("member")
                    .record_type = "source".into()
            }
            "version" => {
                changed
                    .dependencies
                    .iter_mut()
                    .find(|d| d.role == "selected_member")
                    .expect("member")
                    .observed_updated_at = None
            }
            "subjects" => changed.base.subjects.clear(),
            "limit" => {
                if let KnowledgeViewSelector::Query { max_members, .. } = &mut definition.selector {
                    *max_members = 0;
                }
            }
            _ => unreachable!(),
        }
        assert!(
            article_context_roots(&definition, &changed).is_err(),
            "accepted {mutation}"
        );
    }
}

#[tokio::test]
async fn query_evidence_cannot_survive_changed_or_deleted_original_source() {
    for embedded in [true, false] {
        let fixture = Fixture::new(embedded).await;
        let compiler = fixture.compiler();
        let mut context = fixture.context().await;
        compiler
            .augment_with_entity_source_evidence("space", &fixture.definition, &mut context)
            .await
            .expect("capture original evidence");
        let mut changed = fixture.revision.clone();
        changed.text = "Changed immutable bytes under the old identity".into();
        DocRevisionRepository::new(fixture.storage.clone())
            .update("space", &changed.base.id.clone(), changed)
            .await
            .expect("corrupt source fixture");
        assert!(compiler
            .build_evidence_catalog("space", &mut context.clone())
            .await
            .is_err());
        DocRevisionRepository::new(fixture.storage.clone())
            .update("space", &fixture.revision.base.id, fixture.revision.clone())
            .await
            .expect("restore original fixture");
        SourceRepository::new(fixture.storage.clone())
            .delete("space", &fixture.source.base.id)
            .await
            .expect("delete original source");
        assert!(compiler
            .build_evidence_catalog("space", &mut context)
            .await
            .is_err());
    }
}

#[tokio::test]
async fn revalidated_wiki_rejects_changed_immutable_revision_and_deleted_source() {
    for embedded in [true, false] {
        for delete in [false, true] {
            let fixture = Fixture::new(embedded).await;
            let compiler = fixture.compiler();
            let original = elephant_storage::canonical_commit::CanonicalRepository::new(
                fixture.storage.clone(),
            )
            .current_epoch("space")
            .await
            .expect("epoch");
            let inputs = compiler
                .prepare_article_inputs("space", &fixture.definition.base.id, true, &original)
                .await
                .expect("original grounded inputs");
            let mut fence = WikiInputFence::new(&inputs, &original, true).expect("fence");
            if delete {
                SourceRepository::new(fixture.storage.clone())
                    .delete("space", &fixture.source.base.id)
                    .await
                    .expect("withdraw source");
            } else {
                let mut changed = fixture.revision.clone();
                changed.text = "Changed immutable bytes".into();
                DocRevisionRepository::new(fixture.storage.clone())
                    .update("space", &changed.base.id.clone(), changed)
                    .await
                    .expect("corrupt original");
            }
            assert!(compiler
                .verify_article_input_fence("space", &fixture.definition.base.id, true, &mut fence)
                .await
                .is_err());
            assert_eq!(
                fence.epoch, original,
                "failed recomputation cannot replace original fence"
            );
        }
    }
}

#[tokio::test]
async fn revalidated_wiki_recomputes_query_membership_including_new_matches() {
    for embedded in [true, false] {
        let fixture = Fixture::new(embedded).await;
        let compiler = fixture.compiler();
        let original =
            elephant_storage::canonical_commit::CanonicalRepository::new(fixture.storage.clone())
                .current_epoch("space")
                .await
                .expect("epoch");
        let inputs = compiler
            .prepare_article_inputs("space", &fixture.definition.base.id, true, &original)
            .await
            .expect("original grounded inputs");
        let mut fence = WikiInputFence::new(&inputs, &original, true).expect("fence");
        let mut unrelated = Entity::new(
            base(TableName::Entity),
            EntityType::new("person"),
            "Unrelated inserted person".into(),
        );
        unrelated.summary = Some("Unrelated supported topic".into());
        EntityRepository::new(fixture.storage.clone())
            .create(unrelated)
            .await
            .expect("unrelated");
        compiler
            .verify_article_input_fence("space", &fixture.definition.base.id, true, &mut fence)
            .await
            .expect("unchanged complete selection");
        let accepted = fence.epoch.clone();
        let matching = Entity::new(
            base(TableName::Entity),
            EntityType::new("person"),
            "Eve new match".into(),
        );
        EntityRepository::new(fixture.storage.clone())
            .create(matching)
            .await
            .expect("new query match");
        assert!(
            compiler
                .verify_article_input_fence("space", &fixture.definition.base.id, true, &mut fence)
                .await
                .is_err(),
            "old cited records alone do not prove membership"
        );
        assert_eq!(fence.epoch, accepted);
    }
}

#[tokio::test]
async fn revalidated_wiki_keeps_original_source_envelope_when_bytes_match() {
    for embedded in [true, false] {
        let fixture = Fixture::new(embedded).await;
        let compiler = fixture.compiler();
        let original =
            elephant_storage::canonical_commit::CanonicalRepository::new(fixture.storage.clone())
                .current_epoch("space")
                .await
                .expect("epoch");
        let inputs = compiler
            .prepare_article_inputs("space", &fixture.definition.base.id, true, &original)
            .await
            .expect("original grounded inputs");
        let mut fence = WikiInputFence::new(&inputs, &original, true).expect("fence");
        let mut changed = fixture.source.clone();
        changed.metadata = json!({"source_control": "changed without changing immutable text"});
        SourceRepository::new(fixture.storage.clone())
            .update("space", &changed.base.id.clone(), changed)
            .await
            .expect("real original source update");
        let revision = DocRevisionRepository::new(fixture.storage.clone())
            .get("space", &fixture.revision.base.id)
            .await
            .expect("read original")
            .expect("revision");
        assert_eq!(revision.text, fixture.revision.text);
        assert_eq!(revision.text_hash, fixture.revision.text_hash);
        assert!(
            compiler
                .verify_article_input_fence("space", &fixture.definition.base.id, true, &mut fence)
                .await
                .is_err(),
            "identical immutable bytes do not refresh the original source"
        );
        assert_eq!(fence.epoch, original);
    }
}
