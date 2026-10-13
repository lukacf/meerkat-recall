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

/// Seeds an isolated embedded graph and serves the regular Meerkat Recall API over it.
pub async fn run(port: u16, processing_config: Option<std::path::PathBuf>) -> Result<()> {
    #[cfg(feature = "meerkat-extraction")]
    let processing = load_processing(processing_config.as_deref().context(
        "model-backed demo requires --processing-config PATH with approved wiki_planner, wiki_writer and wiki_verifier bindings; see the bundled service/demo-wiki-processing.example.json",
    )?)?;
    #[cfg(not(feature = "meerkat-extraction"))]
    anyhow::ensure!(
        processing_config.is_none(),
        "--processing-config requires the meerkat-extraction feature"
    );
    let storage = StorageClient::new_embedded()
        .await
        .context("failed to create embedded Meerkat Recall storage")?;
    #[cfg(feature = "meerkat-extraction")]
    seed_processing(&storage, processing).await?;
    let seeded = Box::pin(seed(&storage)).await?;
    let wiki_url = format!("http://localhost:{port}/wiki");
    #[cfg(feature = "meerkat-extraction")]
    let created = {
        let config = elephant_pipeline::wiki_editorial::WikiEditorialConfig::default();
        let candidates = EntityRepository::new(storage.clone())
            .list(SPACE_ID, 250, None)
            .await
            .context("failed to list demo entities for wiki planning")?;
        elephant_pipeline::wiki_editorial::WikiGraphPlanner::with_meerkat(
            storage.clone(),
            SPACE_ID,
            config,
        )
        .await
        .context("demo space provider approvals are unavailable")?
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

#[cfg(feature = "meerkat-extraction")]
fn load_processing(
    path: &std::path::Path,
) -> Result<elephant_types::provider_binding::SpaceProcessingConfig> {
    use std::io::Read;
    let file = std::fs::File::open(path).context("open explicit demo processor configuration")?;
    anyhow::ensure!(
        file.metadata()?.is_file(),
        "demo processor configuration must be a regular file"
    );
    let mut bytes = Vec::new();
    file.take(1_048_577).read_to_end(&mut bytes)?;
    anyhow::ensure!(
        bytes.len() <= 1_048_576,
        "demo processor configuration exceeds 1 MiB"
    );
    let processing: elephant_types::provider_binding::SpaceProcessingConfig =
        serde_json::from_slice(&bytes).context("invalid demo SpaceProcessingConfig JSON")?;
    processing
        .validate()
        .context("invalid demo processor approvals")?;
    Ok(processing)
}

#[cfg(feature = "meerkat-extraction")]
#[expect(
    clippy::significant_drop_tightening,
    reason = "retain the demo approval repository lifetime across all current-operation checks"
)]
async fn seed_processing(
    storage: &StorageClient,
    processing: elephant_types::provider_binding::SpaceProcessingConfig,
) -> Result<()> {
    use elephant_types::provider_binding::ModelOperation;
    let (embedding_provider, embedding_model, embedding_dims) =
        if let Some(id) = &processing.active_embedding_profile_id {
            let profile = processing
                .profiles
                .get(id)
                .context("active embedding profile missing")?;
            let connection = processing
                .connections
                .get(&profile.connection_id)
                .context("active connection missing")?;
            (
                connection.provider.clone(),
                profile.model.clone(),
                i32::try_from(profile.dimensions.context("embedding dimensions missing")?)?,
            )
        } else {
            ("disabled".into(), "none".into(), 0)
        };
    let repository = elephant_storage::SpaceConfigRepository::new(storage.clone());
    repository
        .create(elephant_types::records::SpaceConfig {
            base: demo_base(demo_id(TableName::SpaceConfig, "demo-processing")),
            embedding_provider,
            embedding_model,
            embedding_dims,
            processing: Some(processing),
            purpose: Some("Explicit isolated Living Wiki demonstration".into()),
            skill: None,
        })
        .await
        .context("create explicit demo space approvals")?;
    for operation in [
        ModelOperation::WikiPlanner,
        ModelOperation::WikiWriter,
        ModelOperation::WikiVerifier,
    ] {
        repository
            .authorize_model_operation(SPACE_ID, operation, None)
            .await
            .context("all three demo wiki operations require current approvals")?;
    }
    Ok(())
}

#[allow(clippy::too_many_lines)]
#[expect(
    clippy::significant_drop_tightening,
    reason = "retain the original demo seeding repositories and compiler lifetimes"
)]
async fn seed(storage: &StorageClient) -> Result<SeededDemo> {
    // The intrinsic demo graph still belongs to an explicit named space. Model
    // approvals, when supplied by run(), are already persisted and stay intact.
    let spaces = elephant_storage::SpaceConfigRepository::new(storage.clone());
    if spaces.get_by_space_id(SPACE_ID).await?.is_none() {
        spaces
            .create(elephant_types::records::SpaceConfig {
                base: demo_base(demo_id(TableName::SpaceConfig, "demo-processing")),
                embedding_provider: "disabled".into(),
                embedding_model: "none".into(),
                embedding_dims: 0,
                processing: None,
                purpose: Some("Isolated Living Wiki evidence demonstration".into()),
                skill: None,
            })
            .await
            .context("register intrinsic demo space")?;
    }
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
    source.author = Some("Meerkat Recall project".to_string());
    SourceRepository::new(storage.clone())
        .create(source)
        .await
        .context("failed to seed demo source")?;

    DocRepository::new(storage.clone())
        .create(Doc {
            base: demo_base(doc_id.clone()),
            title: "Meerkat Recall Living Wiki architecture".to_string(),
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
        "Meerkat Recall Living Wiki",
        "A derived, cited, freshness-aware read model over canonical Meerkat Recall knowledge.",
    );
    let graph = demo_entity(
        "canonical-graph",
        "knowledge_system",
        "Canonical Meerkat Recall graph",
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
            subject_text: "Meerkat Recall Living Wiki",
            predicate: "derived_from",
            object_text: "canonical knowledge",
            quote: "Meerkat Recall's Living Wiki is the human-readable semantic projection of canonical\nknowledge.",
        },
        DemoRelationClaim {
            key: "preserves-source-authority",
            subject: &wiki_id,
            object: &evidence_id,
            subject_text: "Meerkat Recall Living Wiki",
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
        "Meerkat Recall Living Wiki",
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
        title: "Meerkat Recall Living Wiki architecture".to_string(),
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

    #[cfg(feature = "meerkat-extraction")]
    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "retain the original demo oracle names and fixture drop lifetimes"
    )]
    async fn explicit_demo_approvals_use_shared_space_authority_without_dispatch() {
        use elephant_types::provider_binding::{ModelOperation, SpaceProcessingConfig};
        let processing: SpaceProcessingConfig = serde_json::from_str(include_str!(
            "../../../deploy/native/demo-wiki-processing.example.json"
        ))
        .expect("processing fixture");
        let storage = StorageClient::new_embedded().await.expect("storage");
        seed_processing(&storage, processing.clone())
            .await
            .expect("explicit approvals");
        let repository = elephant_storage::SpaceConfigRepository::new(storage.clone());
        for operation in [
            ModelOperation::WikiPlanner,
            ModelOperation::WikiWriter,
            ModelOperation::WikiVerifier,
        ] {
            let binding = repository
                .authorize_model_operation(SPACE_ID, operation, None)
                .await
                .expect("approved operation");
            assert_eq!(binding.provenance.config.space_id, SPACE_ID);
            assert!(repository
                .authorize_model_operation("another-space", operation, None)
                .await
                .is_err());
        }
        let mut denied = processing;
        denied
            .bindings
            .get_mut(&ModelOperation::WikiVerifier)
            .expect("verifier")
            .revoked = true;
        let unapproved = StorageClient::new_embedded()
            .await
            .expect("isolated storage");
        assert!(seed_processing(&unapproved, denied).await.is_err());
    }

    #[cfg(feature = "meerkat-extraction")]
    #[test]
    fn demo_configuration_file_is_bounded_and_strict() {
        let directory = tempfile::tempdir().expect("directory");
        let path = directory.path().join("processing.json");
        std::fs::write(
            &path,
            include_str!("../../../deploy/native/demo-wiki-processing.example.json"),
        )
        .expect("write fixture");
        assert!(load_processing(&path).is_ok());
        std::fs::write(&path, vec![b' '; 1_048_577]).expect("oversize fixture");
        assert!(load_processing(&path)
            .expect_err("oversize refused")
            .to_string()
            .contains("exceeds 1 MiB"));
        std::fs::write(&path, b"{}").expect("incomplete fixture");
        assert!(load_processing(&path).is_err());
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "retain the original demo oracle names and fixture drop lifetimes"
    )]
    async fn demo_seed_builds_current_evidence_and_context_projections() {
        let storage = StorageClient::new_embedded().await.expect("storage");
        let result = Box::pin(seed(&storage)).await.expect("seed demo");
        let space = elephant_storage::SpaceConfigRepository::new(storage.clone())
            .get_by_space_id(SPACE_ID)
            .await
            .expect("lookup demo space")
            .expect("explicit named demo space");
        assert!(
            space.processing.is_none(),
            "intrinsic demo seed grants no provider approvals"
        );
        assert_eq!(result.view_count, 6);
        let views = KnowledgeViewRepository::new(storage.clone());
        let view = views
            .get_view(SPACE_ID, &result.context_view_id)
            .await
            .expect("load topic")
            .expect("topic view");
        assert_eq!(
            view.state,
            elephant_types::records::ProjectionState::Current
        );
        let revisions = views
            .list_views(SPACE_ID, 100)
            .await
            .expect("all seeded views");
        for view in revisions {
            let revision = views
                .get_current_revision(&view)
                .await
                .expect("load current projection")
                .expect("compiled projection");
            assert_eq!(
                revision.base.subjects_state,
                SubjectsState::Unknown,
                "source uncertainty remains in every evidence-bearing projection"
            );
            assert!(
                elephant_knowledge_views::projection_is_current(&storage, &revision)
                    .await
                    .expect("current exact evidence")
            );
        }
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        clippy::similar_names,
        reason = "retain the original demo oracle names and fixture drop lifetimes"
    )]
    async fn demo_compilation_inherits_evidence_subjects_and_rejects_stronger_source_policy() {
        let storage = StorageClient::new_embedded().await.expect("storage");
        Box::pin(seed(&storage)).await.expect("seed demo");
        let owner = demo_entity(
            "document-owner",
            "person",
            "Document owner",
            "Source subject",
        );
        let owner_id = owner.base.id.clone();
        EntityRepository::new(storage.clone())
            .create(owner)
            .await
            .expect("intrinsic source subject");
        let sources = SourceRepository::new(storage.clone());
        let mut source = sources
            .get(
                SPACE_ID,
                &demo_id(TableName::Source, "living-wiki-architecture-source"),
            )
            .await
            .expect("load source")
            .expect("seeded source");
        source.base.subjects = vec![Link::new(owner_id.clone())];
        source.base.subjects_state = SubjectsState::Partial;
        source.base.updated_at = Utc::now();
        sources
            .update(SPACE_ID, &source.base.id, source.clone())
            .await
            .expect("restrict source subjects");
        let compiler = EntityViewCompiler::new(storage.clone());
        let root = demo_id(TableName::Entity, "living-wiki");
        let compiled =
            Box::pin(compiler.compile(SPACE_ID, &root, EntityViewLimits::default(), true))
                .await
                .expect("inherit source-only subject");
        assert!(compiled
            .revision
            .base
            .subjects
            .iter()
            .any(|link| link.id() == &owner_id));
        assert_eq!(
            compiled.revision.base.subjects_state,
            SubjectsState::Unknown
        );
        assert!(compiled.revision.dependencies.iter().any(|dependency| {
            dependency.record_id == source.base.id.to_string()
                && dependency.observed_updated_at == Some(source.base.updated_at)
        }));
        source.base.security.level = 1;
        source.base.updated_at = Utc::now();
        sources
            .update(SPACE_ID, &source.base.id, source.clone())
            .await
            .expect("restrict source classification");
        assert!(
            !elephant_knowledge_views::projection_is_current(&storage, &compiled.revision)
                .await
                .expect("old projection rejected")
        );
        assert!(
            Box::pin(compiler.compile(SPACE_ID, &root, EntityViewLimits::default(), true))
                .await
                .is_err(),
            "source classification cannot widen into the root security partition"
        );
        assert_eq!(
            sources
                .get(SPACE_ID, &source.base.id)
                .await
                .expect("unchanged source"),
            Some(source)
        );
    }
}
