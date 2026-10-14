//! Retained editorial compiler with a fixed, independently constrained writer/verifier.
//! No paid provider, broad approving model stub, or synthetic published view fixture.

#[path = "../../recall_preview/graph_support.rs"]
mod graph_support;
#[path = "../../recall_preview/publication_support.rs"]
mod publication_support;
#[path = "../../recall_preview/support.rs"]
mod support;

use elephant_pipeline::wiki_editorial::{
    EditorialDraft, EditorialSection, EditorialStatement, EditorialVerification,
    StatementVerificationDecision, WikiEditorialCompiler, WikiEditorialConfig, WikiEditorialLlm,
    WikiOrganization, WikiPlan,
};
use elephant_pipeline::{PipelineError, PipelineResult};
use elephant_storage::canonical_commit::CanonicalRepository;
use elephant_storage::read_eligibility::ReadEligibility;
use elephant_storage::source_publication::{SourceMutation, SourcePublicationRepository};
use elephant_storage::{
    AssertionRepository, EntityRepository, KnowledgeViewDefinitionRepository,
    KnowledgeViewRepository, Repository,
};
use elephant_types::publication::SourcePrecondition;
use elephant_types::records::{
    BaseFields, KnowledgeViewDefinition, KnowledgeViewPublicationStatus, KnowledgeViewSelector,
    Security,
};
use elephant_types::{Link, RecordId, TableName};
use graph_support::GraphFixture;
use serde_json::Value;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

const SPACE: &str = "wiki-qualification";
const SECTION: &str = "On 5 October 2026, Élodie opened the Oriole laboratory.";

#[derive(Default)]
struct WriterGate {
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
}

struct FixedEditorial {
    assertion: RecordId,
    gate: Option<Arc<WriterGate>>,
    writer_calls: AtomicUsize,
    verifier_calls: AtomicUsize,
}

fn invalid(message: &str) -> PipelineError {
    PipelineError::ValidationError(message.into())
}

impl FixedEditorial {
    fn citation(&self, prompt: &str) -> PipelineResult<String> {
        let (_, text) = prompt
            .rsplit_once("EVIDENCE CATALOG:\n")
            .ok_or_else(|| invalid("fixture catalog absent"))?;
        let catalog: Vec<Value> =
            serde_json::from_str(text).map_err(|_| invalid("fixture catalog malformed"))?;
        catalog
            .iter()
            .find(|row| {
                row["kind"] == "evidence_span"
                    && row["source_quote"] == support::TEXT
                    && row["record_id"] == self.assertion.to_string()
            })
            .and_then(|row| row["citation_id"].as_str())
            .map(str::to_owned)
            .ok_or_else(|| invalid("no independently expected immutable Oriole passage in catalog"))
    }
}

#[async_trait::async_trait]
impl WikiEditorialLlm for FixedEditorial {
    async fn plan_wiki(&self, _prompt: String) -> PipelineResult<WikiPlan> {
        Err(invalid("planner not part of this fixture"))
    }
    async fn organize_wiki(&self, _prompt: String) -> PipelineResult<WikiOrganization> {
        Err(invalid("organizer not part of this fixture"))
    }
    async fn write_article(&self, prompt: String) -> PipelineResult<EditorialDraft> {
        let citation = self.citation(&prompt)?;
        self.writer_calls.fetch_add(1, Ordering::SeqCst);
        if let Some(gate) = &self.gate {
            gate.entered.notify_one();
            gate.release.notified().await;
        }
        Ok(EditorialDraft {
            language: "en".into(),
            title: "Élodie and the Oriole opening".into(),
            lead: EditorialStatement {
                id: "lead-1".into(),
                text: support::TEXT.into(),
                citation_ids: vec![citation.clone()],
            },
            sections: vec![EditorialSection {
                id: "opening".into(),
                heading: "Laboratory opening".into(),
                kind: "overview".into(),
                statements: vec![EditorialStatement {
                    id: "opening-1".into(),
                    text: SECTION.into(),
                    citation_ids: vec![citation],
                }],
            }],
        })
    }
    async fn verify_article(&self, prompt: String) -> PipelineResult<EditorialVerification> {
        let citation = self.citation(&prompt)?;
        let (_, draft) = prompt
            .split_once("ARTICLE DRAFT:\n")
            .ok_or_else(|| invalid("draft absent"))?;
        let (draft, _) = draft
            .split_once("\n\nEVIDENCE CATALOG:")
            .ok_or_else(|| invalid("draft/catalog boundary absent"))?;
        let draft: EditorialDraft =
            serde_json::from_str(draft).map_err(|_| invalid("draft malformed"))?;
        if draft.lead.text != support::TEXT
            || draft.lead.id != "lead-1"
            || draft.lead.citation_ids != vec![citation.clone()]
            || draft.sections.len() != 1
            || draft.sections[0].statements.len() != 1
            || draft.sections[0].statements[0].text != SECTION
            || draft.sections[0].statements[0].id != "opening-1"
            || draft.sections[0].statements[0].citation_ids != vec![citation]
        {
            return Err(invalid(
                "fixture verifier refuses changed or unsupported semantics",
            ));
        }
        self.verifier_calls.fetch_add(1, Ordering::SeqCst);
        Ok(EditorialVerification {
            decisions: ["lead-1", "opening-1"]
                .into_iter()
                .map(|id| StatementVerificationDecision {
                    statement_id: id.into(),
                    entailed: true,
                    relevant: true,
                    reason:
                        "Exact fixed opening fact independently matches original revision bytes"
                            .into(),
                })
                .collect(),
            article_useful: true,
            article_coherent: true,
            temporal_framing_sound: true,
            missing_major_topics: vec![],
            article_reason: "One historical opening fact; no broader inference".into(),
        })
    }
    fn writer_model(&self) -> &str {
        "qualification-fixed-writer"
    }
    fn verifier_model(&self) -> &str {
        "qualification-fixed-verifier"
    }
}

async fn fixture(backend: support::Backend) -> anyhow::Result<(GraphFixture, RecordId)> {
    let client = support::storage(backend).await?;
    let graph = GraphFixture::new(client.clone(), SPACE).await?;
    graph.index_fixture_vectors().await?;
    graph.publication.finalize().await?;
    let root = EntityRepository::new(client.clone())
        .list(SPACE, 100, None)
        .await?
        .into_iter()
        .find(|row| row.display_name == "Élodie")
        .ok_or_else(|| anyhow::anyhow!("published subject absent"))?;
    let definition = KnowledgeViewDefinition {
        base: support::classified_base(TableName::KnowledgeViewDefinition, SPACE),
        title: "Élodie and the Oriole opening".into(),
        publication_status: KnowledgeViewPublicationStatus::Published,
        related_articles: vec![],
        navigation: None,
        selector: KnowledgeViewSelector::Topic {
            topic: Link::new(root.base.id),
            relation_types: vec!["qualification-no-relations".into()],
            max_hops: 1,
            max_members: 10,
        },
    };
    let id = definition.base.id.clone();
    KnowledgeViewDefinitionRepository::new(client)
        .upsert(definition)
        .await?;
    Ok((graph, id))
}

async fn compiler(
    graph: &GraphFixture,
    gate: Option<Arc<WriterGate>>,
) -> anyhow::Result<(WikiEditorialCompiler, Arc<FixedEditorial>)> {
    let assertion = AssertionRepository::new(graph.publication.client.clone())
        .list(SPACE, 100, None)
        .await?
        .into_iter()
        .find(|row| row.evidence.revision.id() == &graph.lineage.revision.base.id)
        .ok_or_else(|| anyhow::anyhow!("published exact assertion missing"))?;
    let llm = Arc::new(FixedEditorial {
        assertion: assertion.base.id,
        gate,
        writer_calls: AtomicUsize::new(0),
        verifier_calls: AtomicUsize::new(0),
    });
    let config = WikiEditorialConfig {
        writer_model: "qualification-fixed-writer".into(),
        verifier_model: "qualification-fixed-verifier".into(),
        max_output_tokens: 1000,
        structured_output_retries: 0,
        timeout_secs: 5,
        max_context_chars: 32_000,
        max_editorial_attempts: 1,
    };
    Ok((
        WikiEditorialCompiler::new(graph.publication.client.clone(), llm.clone(), config),
        llm,
    ))
}

#[tokio::test]
async fn classified_editorial_article_and_cached_read_keep_exact_original_evidence(
) -> anyhow::Result<()> {
    for backend in support::BACKENDS {
        let (graph, definition) = fixture(backend).await?;
        let (compiler, llm) = compiler(&graph, None).await?;
        // Derived assertions may add resolved subjects beyond their source.
        // Freeze the actual contributor requirements before model compilation.
        let assertions = AssertionRepository::new(graph.publication.client.clone())
            .list(SPACE, 100, None)
            .await?;
        let mut required_subjects = graph.lineage.source.base.subjects.clone();
        for assertion in &assertions {
            for subject in &assertion.base.subjects {
                if !required_subjects.contains(subject) {
                    required_subjects.push(subject.clone());
                }
            }
        }
        let article = compiler.compile(SPACE, &definition, true).await?;
        anyhow::ensure!(article.changed && article.revision.validation.statements_supported);
        anyhow::ensure!(
            article.view.base.security == graph.lineage.source.base.security
                && article.revision.base.security == graph.lineage.source.base.security
        );
        for base in [&article.view.base, &article.revision.base] {
            anyhow::ensure!(base.subjects_state == graph.lineage.source.base.subjects_state);
            anyhow::ensure!(
                required_subjects
                    .iter()
                    .all(|subject| base.subjects.contains(subject)),
                "wiki projection dropped a source or derived-assertion subject restriction"
            );
        }
        let evidence = article
            .revision
            .citations
            .iter()
            .filter_map(|citation| citation.evidence_span.as_ref())
            .collect::<Vec<_>>();
        anyhow::ensure!(!evidence.is_empty());
        for span in evidence {
            anyhow::ensure!(
                span.revision.id() == &graph.lineage.revision.base.id
                    && span.start == 0
                    && usize::try_from(span.end)? == support::TEXT.len()
                    && span.quote_hash == support::digest(support::TEXT)
            );
        }
        let expected = graph.lineage.source.base.security.clone();
        let authorize = |base: &BaseFields| {
            base.space_id == SPACE && base.security == expected && !base.is_deleted()
        };
        anyhow::ensure!(
            ReadEligibility::new(graph.publication.client.clone())
                .project(article.revision.clone(), &authorize)
                .await?
                .is_some(),
            "model/prompt descriptors or valid context links made complete article unreadable"
        );
        let cached = compiler.compile(SPACE, &definition, true).await?;
        anyhow::ensure!(!cached.changed && cached.revision.base.id == article.revision.base.id);
        anyhow::ensure!(
            llm.writer_calls.load(Ordering::SeqCst) == 1
                && llm.verifier_calls.load(Ordering::SeqCst) == 1
        );
    }
    Ok(())
}

#[tokio::test]
async fn source_retract_or_replacement_after_writer_start_prevents_stale_wiki_publication(
) -> anyhow::Result<()> {
    for backend in support::BACKENDS {
        for change in ["retract", "replace"] {
            let (graph, definition) = fixture(backend).await?;
            let gate = Arc::new(WriterGate::default());
            let (compiler, llm) = compiler(&graph, Some(gate.clone())).await?;
            let running =
                tokio::spawn(async move { compiler.compile(SPACE, &definition, true).await });
            tokio::time::timeout(std::time::Duration::from_secs(5), gate.entered.notified())
                .await?;
            let repository = SourcePublicationRepository::new(graph.publication.client.clone());
            let authorized = support::authorized_source_snapshot(
                &repository,
                SPACE,
                &graph.lineage.source.base.id,
            )
            .await?;
            if change == "retract" {
                repository
                    .retract(SourceMutation {
                        authorized_document: None,
                        space_id: SPACE.into(),
                        source_id: graph.lineage.source.base.id.clone(),
                        expected: SourcePrecondition::Generation(
                            graph.publication.accepted.source_generation.clone(),
                        ),
                        idempotency_key: "wiki-race-retract".into(),
                        request_fingerprint: "wiki-race-retract".into(),
                        actor: "qualification".into(),
                        authorized,
                    })
                    .await?;
            } else {
                let mut next = graph.publication.request.clone();
                next.revision_id = "pending-after-writer".into();
                next.idempotency_key = "wiki-race-replace".into();
                next.request_fingerprint = "wiki-race-replace".into();
                next.expected = SourcePrecondition::Generation(
                    graph.publication.accepted.source_generation.clone(),
                );
                next.authorized = Some(authorized);
                repository.accept(next).await?;
            }
            gate.release.notify_one();
            let result = tokio::time::timeout(std::time::Duration::from_secs(5), running).await??;
            anyhow::ensure!(
                result.is_err(),
                "{backend:?}/{change}: stale writer output published"
            );
            anyhow::ensure!(
                llm.writer_calls.load(Ordering::SeqCst) == 1,
                "test never reached actual writer"
            );
            anyhow::ensure!(
                KnowledgeViewRepository::new(graph.publication.client.clone())
                    .list_views(SPACE, 100)
                    .await?
                    .is_empty(),
                "failed editorial compilation left partial context/article publication"
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn view_publication_refuses_zero_span_stale_hash_and_consistent_policy_downgrade(
) -> anyhow::Result<()> {
    for backend in support::BACKENDS {
        for mutation in ["zero-span", "stale-hash", "default-policy"] {
            let (graph, definition) = fixture(backend).await?;
            let (compiler, _) = compiler(&graph, None).await?;
            let article = compiler.compile(SPACE, &definition, true).await?;
            let client = &graph.publication.client;
            let observed_epoch = CanonicalRepository::new(client.clone())
                .epoch(SPACE)
                .await?;
            let mut view = article.view.clone();
            let mut revision = article.revision.clone();
            revision.base.id = RecordId::new_v4(TableName::KnowledgeViewRevision);
            revision.revision += 1;
            revision.supersedes = Some(article.revision.base.id.to_string());
            view.current_revision = Some(revision.base.id.to_string());
            view.revision_count = revision.revision;
            if mutation == "default-policy" {
                let security = Security {
                    policy_id: "default".into(),
                    level: 0,
                    labels: vec![],
                    handling: vec![],
                };
                // Keep both redundant envelope fingerprints consistent so a mere
                // header mismatch cannot make the downgrade negative pass.
                let fingerprint = format!(
                    "sha256:{}",
                    support::digest(&serde_json::to_string(&security)?)
                );
                view.base.security = security.clone();
                revision.base.security = security;
                view.security_fingerprint = fingerprint.clone();
                revision.compiler.policy_fingerprint = fingerprint;
            } else {
                let span = revision
                    .citations
                    .iter_mut()
                    .find_map(|citation| citation.evidence_span.as_mut())
                    .ok_or_else(|| anyhow::anyhow!("positive article has no evidence"))?;
                if mutation == "zero-span" {
                    span.end = span.start;
                } else {
                    span.quote_hash = support::digest("different immutable quote");
                }
            }
            let revision_id = revision.base.id.clone();
            let views = KnowledgeViewRepository::new(client.clone());
            anyhow::ensure!(
                views
                    .publish_at_epoch(view, revision, &observed_epoch)
                    .await
                    .is_err(),
                "{backend:?}/{mutation}: malformed/underclassified projection published"
            );
            anyhow::ensure!(views.get_revision(SPACE, &revision_id).await?.is_none());
            let live_view = views
                .get_view(SPACE, &article.view.base.id)
                .await?
                .ok_or_else(|| anyhow::anyhow!("previous view head lost"))?;
            anyhow::ensure!(live_view.current_revision == article.view.current_revision);
            let retained = views
                .get_current_revision(&live_view)
                .await?
                .ok_or_else(|| anyhow::anyhow!("previous valid article lost"))?;
            anyhow::ensure!(retained.base.id == article.revision.base.id);
        }
    }
    Ok(())
}
