//! Self-contained demonstration of Living Wiki evidence and context inputs.

use anyhow::{Context, Result};
use chrono::Utc;
use elephant_api::config::ApiConfig;
use elephant_api::AuthConfig;
use elephant_knowledge_views::{
    EntityViewCompiler, EntityViewLimits, GraphViewCompiler, SourceViewCompiler, SourceViewLimits,
};
use elephant_storage::{
    AssertionRepository, AttributeRepository, DocRepository, DocRevisionRepository,
    EntityRepository, KnowledgeViewDefinitionRepository, KnowledgeViewRepository,
    PredicateRegistryRepository, RelRepository, Repository, SourceRepository, StorageClient,
};
use elephant_types::records::attribute::Literal;
use elephant_types::records::base::{BaseFields, SubjectsState};
use elephant_types::records::doc_revision::RendererMetadata;
use elephant_types::records::predicate::{PredicateSemantics, SemanticsStatus};
use elephant_types::records::{
    Assertion, AssertionRetryState, Attribute, ClaimStatus, Doc, DocRevision, Entity, EntityType,
    EvidenceRef, EvidenceSpan, IngestMode, KnowledgeViewDefinition, KnowledgeViewPublicationStatus,
    KnowledgeViewSelector, Rel, ResolutionStatus, RevisionStatus, Source, SourceLifecycleStatus,
    SourceType, TimeBasis,
};
use elephant_types::{Link, RecordId, TableName};
use sha2::{Digest, Sha256};
use uuid::Uuid;

const SPACE_ID: &str = "default";
const SOURCE_TEXT: &str = include_str!("../../../docs/architecture/living-wiki.mdx");

struct SeededDemo {
    view_count: usize,
    context_view_id: RecordId,
}

/// Seeds an isolated embedded graph and serves the regular Elephant API over it.
pub async fn run(port: u16) -> Result<()> {
    let storage = StorageClient::new_embedded()
        .await
        .context("failed to create embedded Elephant storage")?;
    let seeded = Box::pin(seed(&storage)).await?;
    let wiki_url = format!("http://localhost:{port}/wiki");
    #[cfg(feature = "meerkat-extraction")]
    let created = {
        let config = elephant_pipeline::wiki_editorial::WikiEditorialConfig::default();
        let candidates = EntityRepository::new(storage.clone())
            .list(SPACE_ID, 250, None)
            .await
            .context("failed to list demo entities for wiki planning")?;
        elephant_pipeline::wiki_editorial::WikiGraphPlanner::with_meerkat(storage.clone(), config)
            .create_from_entities(
                SPACE_ID,
                candidates,
                elephant_pipeline::wiki_editorial::WikiCreationConfig {
                    max_articles: 4,
                    max_related_articles: 3,
                    article_max_hops: 1,
                    article_max_members: 20,
                    publish: true,
                },
            )
            .await
            .context("failed to plan, write, and verify the demo wiki")?
    };
    let context_url = format!(
        "http://localhost:{port}/inspect/knowledge-views/{}",
        seeded.context_view_id
    );
    #[cfg(feature = "meerkat-extraction")]
    for article in &created.articles {
        println!("LLM-authored article: {wiki_url}/{}", article.view.base.id);
    }
    #[cfg(feature = "meerkat-extraction")]
    for failure in &created.failures {
        println!(
            "Article blocked for {}: {}",
            failure.entity_id, failure.error
        );
    }
    #[cfg(not(feature = "meerkat-extraction"))]
    println!("Article wiki (empty without meerkat-extraction): {wiki_url}");
    println!("Seeded {} cited views", seeded.view_count);
    println!("Editorial context projection: {context_url}");
    println!("Press Ctrl-C to stop the demo server");

    let auth = AuthConfig {
        skip_auth: true,
        ..AuthConfig::default()
    };
    elephant_api::run_api_with_client_and_shutdown(
        ApiConfig::default().with_host("127.0.0.1").with_port(port),
        auth,
        storage,
        None,
    )
    .await
}

#[allow(clippy::too_many_lines)]
async fn seed(storage: &StorageClient) -> Result<SeededDemo> {
    let now = Utc::now();
    let source_id = demo_id(TableName::Source, "living-wiki-architecture-source");
    let doc_id = demo_id(TableName::Doc, "living-wiki-architecture-doc");
    let revision_id = demo_id(TableName::DocRevision, "living-wiki-architecture-revision");

    let mut source = Source::new(
        demo_base(source_id.clone()),
        SourceType::Manual,
        sha256(SOURCE_TEXT.as_bytes()),
    );
    source.lifecycle_status = SourceLifecycleStatus::GraphReady;
    source.current_ingest_mode = Some(IngestMode::Graph);
    source.external_id = Some("elephant:docs/architecture/living-wiki.mdx".to_string());
    source.path = Some("docs/architecture/living-wiki.mdx".to_string());
    source.author = Some("Elephant project".to_string());
    SourceRepository::new(storage.clone())
        .create(source)
        .await
        .context("failed to seed demo source")?;

    DocRepository::new(storage.clone())
        .create(Doc {
            base: demo_base(doc_id.clone()),
            title: "Elephant Living Wiki architecture".to_string(),
            text: SOURCE_TEXT.to_string(),
            sidecar_text: None,
            source: Some(Link::new(source_id.clone())),
            media_attachments: Vec::new(),
            rendition_key: "living-wiki-demo-v1".to_string(),
            metadata: serde_json::json!({"demo": true}),
        })
        .await
        .context("failed to seed demo document")?;
    DocRevisionRepository::new(storage.clone())
        .create(DocRevision {
            base: demo_base(revision_id.clone()),
            doc: Link::new(doc_id),
            text: SOURCE_TEXT.to_string(),
            text_hash: sha256(SOURCE_TEXT.as_bytes()),
            renderer: RendererMetadata {
                name: "elephant.demo.source".to_string(),
                version: "1".to_string(),
                config_hash: None,
            },
            manifest: None,
            supersedes: None,
            status: RevisionStatus::Active,
            rendered_at: now,
        })
        .await
        .context("failed to seed immutable demo revision")?;

    let mut wiki = demo_entity(
        "living-wiki",
        "topic",
        "Elephant Living Wiki",
        "A derived, cited, freshness-aware read model over canonical Elephant knowledge.",
    );
    let graph = demo_entity(
        "canonical-graph",
        "knowledge_system",
        "Canonical Elephant graph",
        "The authoritative graph of entities, relationships, attributes, and truth state.",
    );
    let evidence = demo_entity(
        "immutable-evidence",
        "knowledge_system",
        "Immutable source evidence",
        "Source documents, immutable revisions, and byte-addressed evidence spans.",
    );
    let revision = demo_entity(
        "view-revision",
        "artifact",
        "Knowledge view revision",
        "An immutable compiled page with statements, citations, dependencies, and receipts.",
    );
    wiki.base.subjects = vec![Link::new(wiki.base.id.clone())];
    wiki.base.subjects_state = SubjectsState::Complete;
    let wiki_id = wiki.base.id.clone();
    let graph_id = graph.base.id.clone();
    let evidence_id = evidence.base.id.clone();
    let view_revision_id = revision.base.id.clone();
    let entities = EntityRepository::new(storage.clone());
    for entity in [wiki, graph, evidence, revision] {
        entities
            .create(entity)
            .await
            .context("failed to seed demo entity")?;
    }

    let assertion_repository = AssertionRepository::new(storage.clone());
    let relation_repository = RelRepository::new(storage.clone());
    for claim in [
        DemoRelationClaim {
            key: "derived-from-graph",
            subject: &wiki_id,
            object: &graph_id,
            subject_text: "Elephant Living Wiki",
            predicate: "derived_from",
            object_text: "canonical knowledge",
            quote: "Elephant's Living Wiki is the human-readable semantic projection of canonical\nknowledge.",
        },
        DemoRelationClaim {
            key: "preserves-source-authority",
            subject: &wiki_id,
            object: &evidence_id,
            subject_text: "Elephant Living Wiki",
            predicate: "preserves_authority_of",
            object_text: "immutable source evidence",
            quote: "the canonical graph and immutable source evidence remain authoritative",
        },
        DemoRelationClaim {
            key: "publishes-revisions",
            subject: &wiki_id,
            object: &view_revision_id,
            subject_text: "stable knowledge view",
            predicate: "publishes",
            object_text: "immutable knowledge view revision",
            quote: "A stable `knowledge_view` record provides identity, security scope, current\nstate, and a pointer to an immutable `knowledge_view_revision`.",
        },
    ] {
        let assertion = demo_assertion(
            claim.key,
            claim.subject_text,
            claim.predicate,
            Some(claim.object_text),
            None,
            claim.subject,
            Some(claim.object),
            &source_id,
            &revision_id,
            claim.quote,
            now,
        )?;
        let evidence_ref = EvidenceRef {
            assertion: Link::new(assertion.base.id.clone()),
            span: assertion.evidence.clone(),
            time_basis: TimeBasis::ObservedAt,
            time_value: now,
        };
        assertion_repository
            .create(assertion)
            .await
            .context("failed to seed demo assertion")?;
        relation_repository
            .create(Rel {
                base: demo_base(demo_id(TableName::Rel, claim.key)),
                subject: Link::new(claim.subject.clone()),
                object: Link::new(claim.object.clone()),
                rel_type: claim.predicate.to_string(),
                status: ClaimStatus::Approved,
                confidence: 1.0,
                validity_window: None,
                evidence: vec![evidence_ref],
                temporal_audit: None,
            })
            .await
            .context("failed to seed demo relationship")?;
    }
    let predicate_registry = PredicateRegistryRepository::new(storage.clone());
    for predicate in ["derived_from", "preserves_authority_of", "publishes"] {
        predicate_registry
            .upsert_semantics(
                SPACE_ID,
                predicate,
                PredicateSemantics::default(),
                SemanticsStatus::Configured,
                "Curated Living Wiki demo predicate",
            )
            .await
            .context("failed to seed governed demo predicate")?;
    }

    let policy_quote = "Factual prose is not an unstructured model completion.";
    let policy_assertion = demo_assertion(
        "factual-prose-policy",
        "Elephant Living Wiki",
        "factual_prose_policy",
        None,
        Some(serde_json::Value::String(
            "Every claim-bearing passage has a statement-local support receipt".to_string(),
        )),
        &wiki_id,
        None,
        &source_id,
        &revision_id,
        policy_quote,
        now,
    )?;
    let policy_evidence = EvidenceRef {
        assertion: Link::new(policy_assertion.base.id.clone()),
        span: policy_assertion.evidence.clone(),
        time_basis: TimeBasis::ObservedAt,
        time_value: now,
    };
    assertion_repository
        .create(policy_assertion)
        .await
        .context("failed to seed demo policy assertion")?;
    AttributeRepository::new(storage.clone())
        .create(Attribute {
            base: demo_base(demo_id(TableName::Attribute, "factual-prose-policy")),
            subject: Link::new(wiki_id.clone()),
            attr_type: "factual_prose_policy".to_string(),
            value: Literal::String(
                "Every claim-bearing passage has a statement-local support receipt".to_string(),
            ),
            status: ClaimStatus::Approved,
            confidence: 1.0,
            validity_window: None,
            evidence: vec![policy_evidence],
            temporal_audit: None,
        })
        .await
        .context("failed to seed demo attribute")?;

    let mut definition_base = demo_base(demo_id(
        TableName::KnowledgeViewDefinition,
        "living-wiki-topic-definition",
    ));
    definition_base.subjects = vec![Link::new(wiki_id.clone())];
    definition_base.subjects_state = SubjectsState::Complete;
    let definition = KnowledgeViewDefinition {
        base: definition_base,
        title: "Elephant Living Wiki architecture".to_string(),
        publication_status: KnowledgeViewPublicationStatus::Published,
        related_articles: Vec::new(),
        navigation: None,
        selector: KnowledgeViewSelector::Topic {
            topic: Link::new(wiki_id.clone()),
            relation_types: Vec::new(),
            max_hops: 1,
            max_members: 20,
        },
    };
    let definition_id = definition.base.id.clone();
    KnowledgeViewDefinitionRepository::new(storage.clone())
        .upsert(definition)
        .await
        .context("failed to seed demo topic definition")?;

    let entity_compiler = EntityViewCompiler::new(storage.clone());
    for entity_id in [&wiki_id, &graph_id, &evidence_id, &view_revision_id] {
        entity_compiler
            .compile(SPACE_ID, entity_id, EntityViewLimits::default(), false)
            .await
            .context("failed to compile demo entity projection")?;
    }
    SourceViewCompiler::new(storage.clone())
        .compile(SPACE_ID, &source_id, SourceViewLimits::default(), false)
        .await
        .context("failed to compile demo source projection")?;
    let topic = GraphViewCompiler::new(storage.clone())
        .compile(SPACE_ID, &definition_id, false)
        .await
        .context("failed to compile demo topic context")?;
    let views = KnowledgeViewRepository::new(storage.clone())
        .list_views(SPACE_ID, 100)
        .await
        .context("failed to list seeded demo views")?;
    Ok(SeededDemo {
        view_count: views.len(),
        context_view_id: topic.view.base.id,
    })
}

struct DemoRelationClaim<'a> {
    key: &'a str,
    subject: &'a RecordId,
    object: &'a RecordId,
    subject_text: &'a str,
    predicate: &'a str,
    object_text: &'a str,
    quote: &'a str,
}

#[allow(clippy::too_many_arguments)]
fn demo_assertion(
    key: &str,
    subject_text: &str,
    predicate: &str,
    object_text: Option<&str>,
    object_literal: Option<serde_json::Value>,
    subject: &RecordId,
    object: Option<&RecordId>,
    source: &RecordId,
    revision: &RecordId,
    quote: &str,
    now: chrono::DateTime<Utc>,
) -> Result<Assertion> {
    let mut base = demo_base(demo_id(TableName::Assertion, key));
    base.provenance.sources = vec![source.clone()];
    base.subjects = std::iter::once(Link::new(subject.clone()))
        .chain(object.map(|id| Link::new(id.clone())))
        .collect();
    base.subjects_state = SubjectsState::Complete;
    Ok(Assertion {
        base,
        subject_text: subject_text.to_string(),
        predicate_raw: predicate.to_string(),
        predicate_id: Some(predicate.to_string()),
        object_text: object_text.map(str::to_string),
        object_literal,
        evidence: evidence_span(revision, quote)?,
        source_event_time: None,
        observed_at: now,
        asserted_at: now,
        assertion_key: format!("demo:{key}"),
        resolution_status: ResolutionStatus::Resolved,
        resolution_method: Some("curated_demo_fixture".to_string()),
        resolution_confidence: Some(1.0),
        retry_state: AssertionRetryState::default(),
        subject_entity: Some(Link::new(subject.clone())),
        object_entity: object.map(|id| Link::new(id.clone())),
        confidence: 1.0,
        subject_entity_type: None,
        object_entity_type: None,
        significance: Some("high".to_string()),
        lifecycle_tier: "active".to_string(),
    })
}

fn evidence_span(revision: &RecordId, quote: &str) -> Result<EvidenceSpan> {
    let start = SOURCE_TEXT
        .find(quote)
        .with_context(|| format!("demo quote is absent from source text: {quote}"))?;
    let end = start + quote.len();
    Ok(EvidenceSpan {
        revision: Link::new(revision.clone()),
        node: None,
        start: u32::try_from(start).context("demo evidence start exceeds u32")?,
        end: u32::try_from(end).context("demo evidence end exceeds u32")?,
        quote_hash: sha256(quote.as_bytes()),
    })
}

fn demo_entity(key: &str, entity_type: &str, name: &str, summary: &str) -> Entity {
    let mut entity = Entity::new(
        demo_base(demo_id(TableName::Entity, key)),
        EntityType::new(entity_type),
        name.to_string(),
    );
    entity.summary = Some(summary.to_string());
    entity
}

fn demo_base(id: RecordId) -> BaseFields {
    BaseFields::new(id, SPACE_ID.to_string(), "elephant.demo".to_string())
}

fn demo_id(table: TableName, key: &str) -> RecordId {
    RecordId::new(
        table,
        Uuid::new_v5(
            &Uuid::NAMESPACE_URL,
            format!("elephant-demo:{key}").as_bytes(),
        ),
    )
}

fn sha256(bytes: &[u8]) -> String {
    format!("sha256:{}", hex::encode(Sha256::digest(bytes)))
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used)]

    use super::*;

    #[tokio::test]
    async fn demo_seed_builds_current_evidence_and_context_projections() {
        let storage = StorageClient::new_embedded().await.expect("storage");
        let result = Box::pin(seed(&storage)).await.expect("seed demo");
        assert_eq!(result.view_count, 6);
        let view = KnowledgeViewRepository::new(storage)
            .get_view(SPACE_ID, &result.context_view_id)
            .await
            .expect("load topic")
            .expect("topic view");
        assert_eq!(
            view.state,
            elephant_types::records::ProjectionState::Current
        );
    }
}
