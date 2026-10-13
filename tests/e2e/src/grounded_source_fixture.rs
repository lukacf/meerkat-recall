//! Real accepted-source/runtime/finalizer fixtures with loopback transport only.
//! Fixed embedding responses test publication plumbing, not model semantics or
//! search quality. Publication authority comes only from the actual pipeline.
//! Negative controls below corrupt and restore already-completed private proof
//! payloads; they never fabricate successful publication authority.
#![allow(dead_code, clippy::missing_errors_doc, clippy::missing_panics_doc)]

use std::sync::Arc;

use anyhow::Context;
use elephant_pipeline::extraction::{
    verify_assertion_support, AssertionSupportVerifier, AssertionValidator, DefaultExtractor,
    ExtractionContext, ExtractionResult, Extractor, RawAssertion, SupportCandidate, SupportVerdict,
};
use elephant_pipeline::provider_dispatch::with_dispatch_credentials;
use elephant_storage::source_intake::{AcceptedSourceIntake, IntakeControl, PreparedSourceIntake};
use elephant_storage::{Repository, SourceRepository, SpaceConfigRepository, StorageClient};
use elephant_types::publication::OperationState;
use elephant_types::records::assertion::EvidenceSpan;
use elephant_types::records::base::BaseFields;
use elephant_types::records::ingest::IngestMode;
use elephant_types::records::source::{ManualDocTemplate, Source, SourceType};
use elephant_types::records::work::PluginManifest;
use elephant_types::{Link, RecordId, TableName};

#[path = "../../recall_preview_semantics/provider_spy.rs"]
mod provider_spy;

pub struct GroundedSourceFixture {
    pub space: String,
    spy: provider_spy::Spy,
}
impl GroundedSourceFixture {
    pub async fn new(storage: &StorageClient, space: &str) -> anyhow::Result<Self> {
        let spy = provider_spy::Spy::start().await?;
        provider_spy::configure(
            &SpaceConfigRepository::new(storage.clone()),
            space,
            &spy.endpoint,
            8,
        )
        .await?;
        Ok(Self {
            space: space.into(),
            spy,
        })
    }

    pub async fn drive(
        &self,
        storage: &StorageClient,
        extractor: &dyn Extractor,
    ) -> anyhow::Result<()> {
        with_dispatch_credentials(
            Arc::new(provider_spy::SpyCredentials),
            Box::pin(crate::pipeline_driver::drive_pipeline(
                storage,
                &self.space,
                extractor,
                300,
            )),
        )
        .await?;
        let requests = self.spy.requests()?;
        anyhow::ensure!(
            !requests.is_empty(),
            "source completion must include actual embedding transport"
        );
        anyhow::ensure!(
            requests
                .iter()
                .all(|request| request.path.ends_with("/embeddings")),
            "fixture must not silently dispatch a generative model"
        );
        Ok(())
    }

    /// Accept and enqueue original source authority, leaving the real worker
    /// stages available for assertions at intermediate boundaries.
    pub async fn accept_text(
        &self,
        storage: &StorageClient,
        title: &str,
        text: &str,
    ) -> anyhow::Result<AcceptedSourceIntake> {
        let mut source = Source::new(
            BaseFields::new(
                RecordId::new_v4(TableName::Source),
                self.space.clone(),
                "grounded-e2e-fixture".into(),
            ),
            SourceType::Manual,
            AssertionValidator::hash_quote(text),
        );
        source.external_id = Some(format!("fixture:{}", uuid::Uuid::new_v4()));
        source.set_manual_doc_template(&ManualDocTemplate {
            doc_id: RecordId::new_v4(TableName::Doc).to_string(),
            title: title.into(),
            text: text.into(),
            sidecar_text: None,
            media_attachments: Vec::new(),
            rendition_key: "fixture-input".into(),
            metadata: serde_json::json!({}),
        })?;
        source.set_requested_ingest_mode(IngestMode::Graph);
        let prepared = Box::pin(PreparedSourceIntake::prepare(
            storage,
            source,
            IntakeControl {
                revision_id: Some("fixture-revision-1".into()),
                idempotency_key: Some(format!("fixture:{}", uuid::Uuid::new_v4())),
                expected_generation: None,
            },
        ))
        .await?;
        let accepted = Box::pin(prepared.accept(storage)).await?;
        elephant_pipeline::source_queue::enqueue_accepted_source(
            storage,
            &accepted.source,
            &accepted.publication,
            "grounded-e2e-fixture",
        )
        .await?;
        Ok(accepted)
    }

    /// Dispatch exactly one queued stage under the same local credential guard.
    pub async fn run_item(
        &self,
        storage: &StorageClient,
        extractor: &dyn Extractor,
        item: &elephant_types::records::work::WorkItem,
    ) -> anyhow::Result<RecordId> {
        with_dispatch_credentials(
            Arc::new(provider_spy::SpyCredentials),
            Box::pin(crate::pipeline_driver::drive_item(
                storage,
                &self.space,
                extractor,
                item,
            )),
        )
        .await
    }

    pub async fn publish_text(
        &self,
        storage: &StorageClient,
        title: &str,
        text: &str,
        extractor: &dyn Extractor,
    ) -> anyhow::Result<AcceptedSourceIntake> {
        let accepted = Box::pin(self.accept_text(storage, title, text)).await?;
        Box::pin(self.drive(storage, extractor)).await?;
        let receipt =
            elephant_storage::source_publication::SourcePublicationRepository::new(storage.clone())
                .operation(&self.space, &accepted.operation.operation_id)
                .await?
                .context("published operation disappeared")?;
        anyhow::ensure!(
            receipt.state == OperationState::Completed,
            "queue completion is not publication: {receipt:?}"
        );
        anyhow::ensure!(
            receipt.published_revision.as_deref()
                == Some(accepted.publication.revision_id.as_str())
        );
        let source = SourceRepository::new(storage.clone())
            .get(&self.space, &accepted.source.base.id)
            .await?
            .context("published source disappeared")?;
        anyhow::ensure!(
            source.lifecycle_status == elephant_types::records::SourceLifecycleStatus::GraphReady
        );
        Ok(accepted)
    }
}

/// Which explicitly identified mention a known-answer fixture reconciles.
#[derive(Clone, Copy)]
pub enum FixtureMentionRole {
    Subject,
    Object,
}

/// Locate one expected mention within one immutable evidence revision. This is
/// a test oracle selecting explicit merge inputs, never production name pooling.
pub async fn fixture_entity_for_revision(
    storage: &StorageClient,
    space: &str,
    revision: &RecordId,
    expected_mention: &str,
    role: FixtureMentionRole,
) -> anyhow::Result<RecordId> {
    let assertions = elephant_storage::AssertionRepository::new(storage.clone())
        .find_by_revision(space, revision, 100)
        .await?;
    anyhow::ensure!(assertions.len() < 100, "fixture assertion bound exceeded");
    let ids = assertions
        .iter()
        .filter_map(|assertion| {
            let (mention, entity) = match role {
                FixtureMentionRole::Subject => (
                    Some(assertion.subject_text.as_str()),
                    assertion.subject_entity.as_ref(),
                ),
                FixtureMentionRole::Object => (
                    assertion.object_text.as_deref(),
                    assertion.object_entity.as_ref(),
                ),
            };
            mention
                .filter(|mention| mention.trim().eq_ignore_ascii_case(expected_mention))
                .and_then(|_| entity.map(|entity| entity.id().clone()))
        })
        .collect::<std::collections::BTreeSet<_>>();
    anyhow::ensure!(
        ids.len() == 1,
        "expected one source-specific entity for {expected_mention:?} in {revision}, got {ids:?}"
    );
    ids.into_iter().next().context("fixture entity disappeared")
}

/// Explicitly reconcile identities through the actual public authorization and
/// atomic canonical merge path. The fixture caller supplies both exact IDs.
pub async fn merge_fixture_entity_ids(
    storage: &StorageClient,
    space: &str,
    survivor: &RecordId,
    merged: &RecordId,
) -> anyhow::Result<elephant_types::records::Entity> {
    anyhow::ensure!(
        survivor != merged,
        "distinct source partitions must exist before explicit merge"
    );
    let principal = elephant_policy::Principal {
        space_id: space.into(),
        scopes: std::collections::HashSet::from(["*".into()]),
        clearances: vec![elephant_policy::Clearance {
            policy_id: "default".into(),
            max_level: i32::MAX,
            labels: Default::default(),
            handling_allow: Default::default(),
        }],
        subject_allowlist: None,
        purpose: Default::default(),
    };
    let response = elephant_mcp::create_dispatcher(storage.clone(), principal)
        .execute("merge_entities", serde_json::json!({
            "entity_a": survivor.to_string(), "entity_b": merged.to_string(), "strategy": "prefer_a"
        })).await?;
    let response: elephant_mcp::ToolResponse<serde_json::Value> = serde_json::from_value(response)?;
    anyhow::ensure!(
        response.ok,
        "explicit identity merge failed: {:?}",
        response.error
    );
    let data = response.data.context("merge response missing data")?;
    anyhow::ensure!(
        data["merged_id"].as_str() == Some(merged.to_string().as_str()),
        "wrong merge input"
    );
    let entity: elephant_types::records::Entity = serde_json::from_value(data["survivor"].clone())?;
    anyhow::ensure!(
        entity.base.id == *survivor,
        "merge did not preserve chosen identity"
    );
    Ok(entity)
}

/// Resolve the actual accepted document's rendered revision after completion.
pub async fn fixture_published_revision(
    storage: &StorageClient,
    accepted: &AcceptedSourceIntake,
) -> anyhow::Result<RecordId> {
    let document = accepted
        .document
        .as_ref()
        .context("source has no document")?;
    let revisions = elephant_storage::DocRevisionRepository::new(storage.clone())
        .find_by_doc(&document.base.id)
        .await?;
    let active = revisions
        .into_iter()
        .filter(|revision| revision.status == elephant_types::records::RevisionStatus::Active)
        .collect::<Vec<_>>();
    anyhow::ensure!(
        active.len() == 1,
        "accepted source needs one immutable active render"
    );
    active
        .into_iter()
        .next()
        .map(|revision| revision.base.id)
        .context("render disappeared")
}

/// Fixed known-answer extraction for the retained small employment scenarios.
/// The independent verifier explicitly checks the source sentence and complete
/// semantic tuple. It cannot approve another relation merely for sharing words.
pub struct EmploymentExtractor {
    manifest: PluginManifest,
}
impl EmploymentExtractor {
    #[expect(
        clippy::new_without_default,
        reason = "retain the explicit known-answer extractor constructor and its original API"
    )]
    pub fn new() -> Self {
        Self {
            manifest: DefaultExtractor::new().manifest().clone(),
        }
    }
}
struct EmploymentSupport;
#[async_trait::async_trait]
impl AssertionSupportVerifier for EmploymentSupport {
    fn identity(&self) -> &str {
        "employment-known-answer-v1"
    }
    async fn verify(
        &self,
        candidates: &[SupportCandidate],
    ) -> Result<Vec<SupportVerdict>, elephant_pipeline::PipelineError> {
        Ok(candidates
            .iter()
            .map(|candidate| SupportVerdict {
                index: candidate.index,
                supported: candidate.quote == "Alice works at Acme Corp."
                    && candidate.claim["subject"] == "Alice"
                    && candidate.claim["predicate"] == "works_at"
                    && candidate.claim["object"] == "Acme Corp"
                    && candidate.claim["literal"].is_null()
                    && candidate.claim["subject_type"] == "person"
                    && candidate.claim["object_type"] == "org"
                    && candidate.claim["subject_entity"].is_null()
                    && candidate.claim["object_entity"].is_null()
                    && candidate.claim["event_precision"].is_null()
                    && candidate.claim["event_lineage"].is_null()
                    && candidate.claim["event_time"].is_null()
                    && candidate.claim["event_title"].is_null(),
            })
            .collect())
    }
}
#[async_trait::async_trait]
impl Extractor for EmploymentExtractor {
    fn manifest(&self) -> &PluginManifest {
        &self.manifest
    }
    async fn extract(
        &self,
        ctx: ExtractionContext,
    ) -> Result<ExtractionResult, elephant_pipeline::PipelineError> {
        let quote = "Alice works at Acme Corp.";
        let Some(start) = ctx.node_text.find(quote) else {
            return Ok(ExtractionResult::empty("employment-known-answer-v1".into()));
        };
        let mut assertions = vec![RawAssertion {
            support_verification: None,
            subject_text: "Alice".into(),
            predicate_text: "works_at".into(),
            object_text: Some("Acme Corp".into()),
            object_literal: None,
            evidence_span: EvidenceSpan {
                revision: Link::new(ctx.revision_id),
                node: Some(Link::new(ctx.node_id)),
                start: u32::try_from(start).map_err(|error| {
                    elephant_pipeline::PipelineError::InvalidInput(error.to_string())
                })?,
                end: u32::try_from(start + quote.len()).map_err(|error| {
                    elephant_pipeline::PipelineError::InvalidInput(error.to_string())
                })?,
                quote_hash: AssertionValidator::hash_quote(quote),
            },
            confidence: 0.99,
            quote: Some(quote.into()),
            source_event_time: None,
            source_event_time_precision: None,
            event_title: None,
            event_lineage_key: None,
            subject_entity_id: None,
            subject_entity_type: Some("person".into()),
            object_entity_id: None,
            object_entity_type: Some("org".into()),
            significance: Some("high".into()),
        }];
        verify_assertion_support(&mut assertions, &ctx.node_text, &EmploymentSupport).await?;
        Ok(ExtractionResult::new(
            assertions,
            0,
            "employment-known-answer-v1".into(),
        ))
    }
}

pub fn principal(space: &str) -> elephant_policy::Principal {
    use std::collections::HashSet;
    elephant_policy::Principal {
        space_id: space.into(),
        scopes: HashSet::from(["*".into()]),
        clearances: vec![elephant_policy::Clearance {
            policy_id: "default".into(),
            max_level: i32::MAX,
            labels: HashSet::new(),
            handling_allow: HashSet::new(),
        }],
        subject_allowlist: None,
        purpose: HashSet::new(),
    }
}

/// Exercise retained explicit extract_doc maintenance through current source
/// acceptance. Each operation finishes through the real dispatcher/finalizer;
/// the second request uses the original head token and a new immutable revision.
pub async fn reextract_employment_via_mcp(
    storage: &StorageClient,
    space: &str,
) -> anyhow::Result<(RecordId, RecordId)> {
    use std::str::FromStr;
    let fixture = GroundedSourceFixture::new(storage, space).await?;
    let dispatcher = elephant_mcp::create_dispatcher(storage.clone(), principal(space));
    let initial = dispatcher
        .execute(
            "ingest_doc_simple",
            serde_json::json!({
                "title": "Manual extraction fixture", "text": "Alice works at Acme Corp.",
                "intake": {"revision_id":"initial", "idempotency_key":"initial-fixture"}
            }),
        )
        .await?;
    anyhow::ensure!(initial["ok"] == true, "intake failed: {initial}");
    let old_doc_id = initial["data"]["data"]["id"]
        .as_str()
        .context("intake omitted document")?;
    let original_generation = initial["operation"]["source_generation"]
        .as_str()
        .context("intake omitted generation")?;
    fixture.drive(storage, &EmploymentExtractor::new()).await?;

    let extracted = dispatcher.execute("extract_doc", serde_json::json!({
        "id": old_doc_id,
        "intake": {"revision_id":"explicit-reextraction", "idempotency_key":"reextraction-fixture",
            "expected_generation": original_generation}
    })).await?;
    anyhow::ensure!(
        extracted["ok"] == true,
        "explicit extraction failed: {extracted}"
    );
    let doc_id = RecordId::from_str(
        extracted["data"]["data"]["id"]
            .as_str()
            .context("extract_doc omitted immutable document")?,
    )?;
    anyhow::ensure!(
        doc_id.to_string() != old_doc_id,
        "replacement must not overwrite old evidence document"
    );
    fixture.drive(storage, &EmploymentExtractor::new()).await?;
    let operation_id = extracted["operation"]["operation_id"]
        .as_str()
        .context("extract_doc omitted operation")?;
    let operation =
        elephant_storage::source_publication::SourcePublicationRepository::new(storage.clone())
            .operation(space, operation_id)
            .await?
            .context("reextraction operation disappeared")?;
    anyhow::ensure!(operation.state == OperationState::Completed);
    anyhow::ensure!(operation.published_revision.as_deref() == Some("explicit-reextraction"));
    let revisions = elephant_storage::DocRevisionRepository::new(storage.clone())
        .find_by_doc(&doc_id)
        .await?;
    let active: Vec<_> = revisions
        .into_iter()
        .filter(|revision| {
            revision.status == elephant_types::records::doc_revision::RevisionStatus::Active
        })
        .collect();
    anyhow::ensure!(
        active.len() == 1,
        "exact immutable document must have one active revision"
    );
    Ok((doc_id, active[0].base.id.clone()))
}

#[cfg(test)]
mod render_proof_tests {
    use super::*;
    use elephant_storage::read_eligibility::ReadEligibility;
    use elephant_storage::source_publication::SourcePublicationRepository;
    use elephant_storage::source_publication_manifest::{
        SourcePublicationCompletion, SourcePublicationManifestRepository,
    };
    use elephant_storage::{DocRepository, DocRevisionRepository};
    use elephant_types::records::doc_revision::RevisionStatus;
    use elephant_types::records::{Doc, DocRevision, KnowledgeViewDefinition};
    use tracing::instrument::WithSubscriber;

    // Scope the diagnostic dispatch to this future's polls, including thread
    // migration, without installing a process-wide subscriber or changing env.
    struct WikiPhaseSubscriber;

    impl tracing::Subscriber for WikiPhaseSubscriber {
        fn enabled(&self, metadata: &tracing::Metadata<'_>) -> bool {
            metadata.is_event()
                && matches!(
                    metadata.target(),
                    "elephant_pipeline::wiki_editorial" | "elephant_pipeline::maintenance_commit"
                )
                && *metadata.level() <= tracing::Level::DEBUG
        }

        fn max_level_hint(&self) -> Option<tracing::level_filters::LevelFilter> {
            Some(tracing::level_filters::LevelFilter::DEBUG)
        }

        fn new_span(&self, _attributes: &tracing::span::Attributes<'_>) -> tracing::span::Id {
            // Spans are disabled by enabled(); only bounded phase events pass.
            tracing::span::Id::from_u64(1)
        }

        fn record(&self, _span: &tracing::span::Id, _values: &tracing::span::Record<'_>) {}

        fn record_follows_from(&self, _span: &tracing::span::Id, _follows: &tracing::span::Id) {}

        fn event(&self, event: &tracing::Event<'_>) {
            if !self.enabled(event.metadata()) {
                return;
            }
            let mut fields = WikiPhaseFields::default();
            event.record(&mut fields);
            if let Some(phase) = fields.phase {
                eprintln!(
                    "render proof commit phase={phase} future_bytes={:?}",
                    fields.future_bytes
                );
            }
        }

        fn enter(&self, _span: &tracing::span::Id) {}

        fn exit(&self, _span: &tracing::span::Id) {}
    }

    #[derive(Default)]
    struct WikiPhaseFields {
        phase: Option<String>,
        future_bytes: Option<u64>,
    }

    impl tracing::field::Visit for WikiPhaseFields {
        fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
            if field.name() == "phase"
                && value.len() <= 96
                && value
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || b"_-.:".contains(&byte))
            {
                self.phase = Some(value.to_owned());
            }
        }

        fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
            if field.name() == "future_bytes" {
                self.future_bytes = Some(value);
            }
        }

        // Ignore messages, IDs, source text and all other unselected fields.
        fn record_debug(&mut self, _field: &tracing::field::Field, _value: &dyn std::fmt::Debug) {}
    }

    // Test diagnostics expose actual child-future size without source text or
    // credentials. Keep large storage projections out of the parent poll frame.
    fn observed_future<F: std::future::Future>(
        phase: &'static str,
        future: F,
    ) -> std::pin::Pin<Box<F>> {
        eprintln!(
            "render proof phase={phase} future_bytes={}",
            std::mem::size_of::<F>()
        );
        Box::pin(future)
    }

    #[test]
    fn render_publication_fixture_future_fits_default_stack_budget() {
        fn assert_size<F, Fut>(_: F)
        where
            F: FnOnce(StorageClient) -> Fut,
            Fut: std::future::Future,
        {
            let size = std::mem::size_of::<Fut>();
            assert!(
                size <= 128 * 1024,
                "render publication fixture future occupies {size} bytes before child poll frames"
            );
        }
        // Inspect the actual fixture type without creating a backend or polling.
        assert_size(|storage| original_render_publication(storage, true));
    }

    struct OriginalDefinitionPlanner {
        alice: String,
    }

    #[async_trait::async_trait]
    impl elephant_pipeline::wiki_editorial::WikiEditorialLlm for OriginalDefinitionPlanner {
        async fn plan_wiki(
            &self,
            prompt: String,
        ) -> elephant_pipeline::PipelineResult<elephant_pipeline::wiki_editorial::WikiPlan>
        {
            use elephant_pipeline::wiki_editorial::{PlannedWikiArticle, WikiPlan};
            if !prompt.contains(&self.alice) {
                return Err(elephant_pipeline::PipelineError::ValidationError(
                    "Original Alice inventory absent".into(),
                ));
            }
            Ok(WikiPlan {
                language: "en".into(),
                articles: vec![PlannedWikiArticle {
                    entity_id: self.alice.clone(),
                    title: "Alice employment".into(),
                    editorial_reason: "Original independently verified employment sentence".into(),
                    supporting_entity_ids: Vec::new(),
                    importance: 3,
                    page_role: "topic".into(),
                    temporal_relevance: "current".into(),
                    related_entity_ids: Vec::new(),
                }],
            })
        }

        async fn organize_wiki(
            &self,
            prompt: String,
        ) -> elephant_pipeline::PipelineResult<elephant_pipeline::wiki_editorial::WikiOrganization>
        {
            use elephant_pipeline::wiki_editorial::{PlannedWikiSection, WikiOrganization};
            if !prompt.contains(&self.alice) {
                return Err(elephant_pipeline::PipelineError::ValidationError(
                    "Original planned article absent".into(),
                ));
            }
            Ok(WikiOrganization {
                language: "en".into(),
                title: "Original employment evidence".into(),
                purpose: "Keep the original supported employment context".into(),
                featured_entity_ids: vec![self.alice.clone()],
                sections: vec![PlannedWikiSection {
                    id: "employment".into(),
                    title: "Employment".into(),
                    description: "Original supported employment".into(),
                    article_entity_ids: vec![self.alice.clone()],
                }],
            })
        }

        async fn write_article(
            &self,
            _prompt: String,
        ) -> elephant_pipeline::PipelineResult<elephant_pipeline::wiki_editorial::EditorialDraft>
        {
            Err(elephant_pipeline::PipelineError::InvalidState(
                "Definition-only oracle does not supply an article writer".into(),
            ))
        }

        async fn verify_article(
            &self,
            _prompt: String,
        ) -> elephant_pipeline::PipelineResult<
            elephant_pipeline::wiki_editorial::EditorialVerification,
        > {
            Err(elephant_pipeline::PipelineError::InvalidState(
                "Definition-only oracle cannot verify an unwritten article".into(),
            ))
        }

        fn writer_model(&self) -> &str {
            "controlled-original-definition-planner"
        }

        fn verifier_model(&self) -> &str {
            "unused-definition-only-verifier"
        }
    }

    async fn commit_original_definition(
        storage: &StorageClient,
        space: &str,
        revision: &RecordId,
    ) -> anyhow::Result<KnowledgeViewDefinition> {
        use elephant_pipeline::wiki_editorial::{
            WikiCreationConfig, WikiEditorialConfig, WikiGraphPlanner,
        };
        use elephant_storage::{EntityRepository, KnowledgeViewDefinitionRepository};
        let alice = fixture_entity_for_revision(
            storage,
            space,
            revision,
            "Alice",
            FixtureMentionRole::Subject,
        )
        .await?;
        let entities = EntityRepository::new(storage.clone());
        let candidates = vec![entities
            .get(space, &alice)
            .await?
            .context("original Alice absent")?];
        let planner = WikiGraphPlanner::new(
            storage.clone(),
            Arc::new(OriginalDefinitionPlanner {
                alice: alice.to_string(),
            }),
            WikiEditorialConfig::default(),
        );
        let created = observed_future(
            "definition_planner",
            planner.create_from_entities(
                space,
                candidates,
                WikiCreationConfig {
                    max_articles: 1,
                    publish: false,
                    ..WikiCreationConfig::default()
                },
            ),
        )
        .with_subscriber(WikiPhaseSubscriber)
        .await?;
        anyhow::ensure!(created.plan.articles.len() == 1 && created.articles.is_empty());
        anyhow::ensure!(
            created.failures.len() == 1,
            "unwritten article cannot succeed"
        );
        anyhow::ensure!(
            elephant_storage::canonical_commit::CanonicalRepository::new(storage.clone())
                .receipt(space, &created.definition_commit_id)
                .await?
                .is_some(),
            "actual planner omitted its original canonical definition receipt"
        );
        let mut definitions = KnowledgeViewDefinitionRepository::new(storage.clone())
            .list(space, 2)
            .await?;
        anyhow::ensure!(
            definitions.len() == 1,
            "expected one actual committed definition"
        );
        Ok(definitions.remove(0))
    }

    // Test-only custody of one already-persisted control payload. Never creates
    // an accepted revision, completion, receipt, or new successful authority.
    #[derive(Clone, Debug, PartialEq)]
    enum OriginalControlRow {
        Embedded(serde_json::Value),
        Native(surrealdb::sql::Thing),
    }

    async fn assert_original_published_source_dependency(
        storage: &StorageClient,
        definition: &KnowledgeViewDefinition,
        accepted: &AcceptedSourceIntake,
    ) -> anyhow::Result<()> {
        let published = SourcePublicationRepository::new(storage.clone())
            .revision(
                &definition.base.space_id,
                &accepted.source.base.id,
                &accepted.publication.revision_id,
            )
            .await?
            .context("original immutable Source absent")?;
        let context = observed_future(
            "graph_context_prepare",
            elephant_knowledge_views::GraphViewCompiler::new(storage.clone()).prepare_context(
                &definition.base.space_id,
                &definition.base.id,
                false,
            ),
        )
        .await?;
        let dependencies = context
            .revision
            .dependencies
            .iter()
            .filter(|dependency| dependency.record_id == accepted.source.base.id.to_string())
            .collect::<Vec<_>>();
        anyhow::ensure!(
            !dependencies.is_empty(),
            "actual graph evidence omitted Source dependency"
        );
        anyhow::ensure!(dependencies.iter().all(|dependency|
            dependency.record_type == "source" && dependency.observed_updated_at == Some(published.source.base.updated_at)),
            "Source dependency must describe exact published semantics, not mutable normalization or pending input");
        anyhow::ensure!(context
            .revision
            .base
            .provenance
            .sources
            .contains(&accepted.source.base.id));
        if !context.persisted {
            // Exercise the actual storage publication validator with the
            // original preparation epoch and unmodified compiler output.
            observed_future(
                "graph_context_publish",
                elephant_storage::KnowledgeViewRepository::new(storage.clone()).publish_at_epoch(
                    context.view,
                    context.revision,
                    &context.observed_epoch,
                ),
            )
            .await?;
        }
        Ok(())
    }

    // Expected public Source comes from the original immutable input and its
    // actual completed Graph run, never from current desired state or projection.
    async fn original_published_source(
        storage: &StorageClient,
        accepted: &AcceptedSourceIntake,
    ) -> anyhow::Result<Source> {
        use elephant_storage::canonical_commit::source_support_fingerprint;
        use elephant_types::records::source::SourceLifecycleStatus;

        let space = &accepted.source.base.space_id;
        let publication = &accepted.publication;
        let publications = SourcePublicationRepository::new(storage.clone());
        let immutable = publications
            .revision(space, &publication.source_id, &publication.revision_id)
            .await?
            .context("actual completed immutable Source absent")?;
        anyhow::ensure!(
            immutable.space_id == *space
                && immutable.source_id == publication.source_id
                && immutable.revision_id == publication.revision_id
                && immutable.source.base.space_id == *space
                && immutable.source.base.id == accepted.source.base.id
                && immutable.source.base.id == publication.source_id
        );
        let manifests = SourcePublicationManifestRepository::new(storage.clone());
        let completion = manifests
            .completion(space, publication)
            .await?
            .context("actual original completion absent")?;
        let manifest = manifests
            .get(space, publication)
            .await?
            .context("actual original manifest absent")?;
        let operation = publications
            .operation(space, &publication.operation_id)
            .await?
            .context("actual original completed operation absent")?;
        anyhow::ensure!(
            completion.space_id == *space
                && completion.publication == *publication
                && completion.accepted_source_fingerprint.as_ref()
                    == Some(&source_support_fingerprint(&immutable)?)
                && manifest.space_id == *space
                && manifest.publication == *publication
                && completion.manifest_digest == manifest.digest()?
                && manifest.mode == IngestMode::Graph
                && operation.space_id == *space
                && operation.source_id == publication.source_id
                && operation.operation_id == publication.operation_id
                && operation.source_generation == publication.generation
                && operation.state == OperationState::Completed
                && operation.completed_at.is_some()
                && operation.desired_revision.as_ref() == Some(&publication.revision_id)
                && operation.published_revision.as_ref() == Some(&publication.revision_id),
            "expected progress must belong to this original completed Graph publication"
        );
        let progress = completion
            .published_progress
            .as_ref()
            .context("actual original completion omitted progress")?;
        anyhow::ensure!(progress.mode == IngestMode::Graph);
        // accept_text enqueues real triage; certified absence is not this fixture.
        let decision_id = progress
            .latest_ingest_decision
            .as_ref()
            .context("actual original triage decision absent")?;
        anyhow::ensure!(decision_id.table() == TableName::IngestDecision);
        let decision = elephant_storage::IngestDecisionRepository::new(storage.clone())
            .get(space, decision_id)
            .await?
            .context("actual original triage decision row absent")?;
        anyhow::ensure!(
            decision.base.id == *decision_id
                && decision.base.space_id == *space
                && decision.base.deleted_at.is_none()
                && decision.source.id() == &publication.source_id
                && decision.decision == IngestMode::Graph
                && progress.ingest_decision_fingerprint.as_ref()
                    == Some(&source_support_fingerprint(&decision)?),
            "original decision identity, source, mode and content must match its certificate"
        );
        let mut expected = immutable.source;
        expected.current_ingest_mode = Some(IngestMode::Graph);
        expected.lifecycle_status = SourceLifecycleStatus::GraphReady;
        expected.latest_ingest_decision = Some(Link::new(decision_id.clone()));
        Ok(expected)
    }

    async fn assert_split_source_envelope_authorization(
        storage: &StorageClient,
        accepted: &AcceptedSourceIntake,
    ) -> anyhow::Result<()> {
        use std::sync::atomic::{AtomicBool, Ordering};

        eprintln!("render proof phase=split_source_authorization_begin");
        let sources = SourceRepository::new(storage.clone());
        let space = &accepted.source.base.space_id;
        let source_id = &accepted.source.base.id;
        let original = sources
            .get(space, source_id)
            .await?
            .context("original live Source absent")?;
        let published = Box::pin(original_published_source(storage, accepted)).await?;
        let mut different_policy = original.clone();
        different_policy.base.security.policy_id = "live-source-only-oracle-policy".into();
        anyhow::ensure!(
            different_policy.base.security.policy_id != published.base.security.policy_id
        );
        sources.update(space, source_id, different_policy).await?;
        let actual = observed_future("split_source_authorization", async {
            let live = sources
                .get(space, source_id)
                .await?
                .context("changed live Source absent")?;
            let published_only = |base: &BaseFields| {
                base.space_id == *space
                    && base.security.policy_id == published.base.security.policy_id
            };
            anyhow::ensure!(published_only(&published.base) && !published_only(&live.base));
            anyhow::ensure!(
                observed_future(
                    "project:live",
                    ReadEligibility::new(storage.clone()).project(live.clone(), &published_only)
                )
                .await?
                .is_none(),
                "published authorization cannot bypass a denied live Source envelope"
            );

            let checked_published = AtomicBool::new(false);
            let live_only = |base: &BaseFields| {
                if base == &published.base {
                    checked_published.store(true, Ordering::SeqCst);
                }
                base.space_id == *space && base.security.policy_id == live.base.security.policy_id
            };
            anyhow::ensure!(live_only(&live.base));
            anyhow::ensure!(
                observed_future(
                    "project:live",
                    ReadEligibility::new(storage.clone()).project(live.clone(), &live_only)
                )
                .await?
                .is_none(),
                "live authorization cannot bypass a denied immutable published envelope"
            );
            anyhow::ensure!(
                checked_published.load(Ordering::SeqCst),
                "the actual published Source must reach the authorization predicate"
            );

            let both = |base: &BaseFields| {
                base.space_id == *space
                    && (base.security.policy_id == live.base.security.policy_id
                        || base.security.policy_id == published.base.security.policy_id)
            };
            anyhow::ensure!(
                observed_future(
                    "project:live",
                    ReadEligibility::new(storage.clone()).project(live.clone(), &both)
                )
                .await?
                .as_ref()
                    == Some(&published),
                "both original envelopes permit exactly immutable content and certified original progress"
            );
            Ok::<(), anyhow::Error>(())
        })
        .await;
        // Restore exactly the captured control row, never an accepted revision,
        // head, completion, receipt or refreshed successful publication proof.
        sources.update(space, source_id, original.clone()).await?;
        actual?;
        let authorize = |base: &BaseFields| base.space_id == *space;
        anyhow::ensure!(
            observed_future(
                "project:original",
                ReadEligibility::new(storage.clone()).project(original, &authorize)
            )
            .await?
            .as_ref()
                == Some(&published)
        );
        Ok(())
    }

    #[derive(Clone, Debug, PartialEq)]
    struct OriginalControlPayload {
        table: &'static str,
        physical_id: String,
        payload: String,
        row: OriginalControlRow,
    }

    impl OriginalControlPayload {
        // Retain every physical original row. The completion table holds an
        // immutable certificate and its separate published-source lookup copy.
        async fn capture(
            storage: &StorageClient,
            table: &'static str,
            space: &str,
            source_id: &RecordId,
        ) -> anyhow::Result<Vec<Self>> {
            let expected_count = match table {
                "source_revision" => 1,
                "source_publication_completion" => 2,
                _ => anyhow::bail!("unsupported original control table"),
            };
            let mut captured = Vec::new();
            if let Some(store) = storage.embedded_store() {
                let rows = store.extension_rows_page(table, space, None, 32).await?;
                anyhow::ensure!(rows.len() < 32, "fixture control page is incomplete");
                let source_key = source_id.to_string();
                for (physical_id, row) in rows.into_iter().filter(|(_, row)| {
                    row.get("source_id").and_then(serde_json::Value::as_str)
                        == Some(source_key.as_str())
                }) {
                    let payload = row
                        .get("payload")
                        .and_then(serde_json::Value::as_str)
                        .context("original payload absent")?
                        .to_owned();
                    captured.push(Self {
                        table,
                        physical_id,
                        payload,
                        row: OriginalControlRow::Embedded(row),
                    });
                }
            } else {
                #[derive(serde::Deserialize)]
                struct NativePayload {
                    id: surrealdb::sql::Thing,
                    payload: String,
                }
                // One more than the largest exact expected set detects extras.
                let mut response = storage
                    .query_bind_checked(
                        &format!("SELECT id, payload FROM {table} WHERE space_id = $space AND source_id = $source LIMIT 3"),
                        elephant_storage::bindings! {
                            "space" => space.to_owned(),
                            "source" => source_id.to_string(),
                        },
                    )
                    .await?;
                let rows: Vec<NativePayload> = response.take(0)?;
                captured.extend(rows.into_iter().map(|row| Self {
                    table,
                    physical_id: row.id.to_string(),
                    payload: row.payload,
                    row: OriginalControlRow::Native(row.id),
                }));
            }
            anyhow::ensure!(
                captured.len() == expected_count,
                "{table}: expected exactly {expected_count} original physical controls"
            );
            captured.sort_by(|left, right| left.physical_id.cmp(&right.physical_id));
            anyhow::ensure!(
                captured
                    .windows(2)
                    .all(|pair| pair[0].physical_id != pair[1].physical_id),
                "original physical control identities must be distinct"
            );
            Ok(captured)
        }

        fn with_payload(&self, payload: &str) -> Self {
            let mut changed = self.clone();
            changed.payload = payload.into();
            if let OriginalControlRow::Embedded(row) = &mut changed.row {
                row["payload"] = serde_json::Value::String(payload.into());
            }
            changed
        }

        async fn write_payload(
            &self,
            storage: &StorageClient,
            payload: &str,
        ) -> anyhow::Result<()> {
            match &self.row {
                OriginalControlRow::Embedded(original) => {
                    let store = storage
                        .embedded_store()
                        .context("original backend changed")?;
                    let mut row = original.clone();
                    row["payload"] = serde_json::Value::String(payload.into());
                    store
                        .mutate(|tx| {
                            tx.upsert_extension_row(self.table, row)?;
                            Ok(())
                        })
                        .await?;
                }
                OriginalControlRow::Native(id) => {
                    storage
                        .query_bind_checked(
                            "UPDATE $id SET payload = $payload",
                            elephant_storage::bindings! {
                                "id" => id.clone(),
                                "payload" => payload.to_owned(),
                            },
                        )
                        .await?;
                }
            }
            Ok(())
        }
    }

    async fn reject_changed_original_controls(
        storage: &StorageClient,
        doc: &Doc,
        revision: &DocRevision,
        accepted: &AcceptedSourceIntake,
        definition: Option<&KnowledgeViewDefinition>,
    ) -> anyhow::Result<()> {
        let space = &doc.base.space_id;
        let source_id = &accepted.source.base.id;
        let authorize = |base: &BaseFields| base.space_id == *space;
        let manifests = SourcePublicationManifestRepository::new(storage.clone());
        let original_completion = manifests
            .completion(space, &accepted.publication)
            .await?
            .context("original completed certificate absent")?;
        anyhow::ensure!(original_completion.space_id == *space);
        anyhow::ensure!(original_completion.publication == accepted.publication);
        let original_revision = SourcePublicationRepository::new(storage.clone())
            .revision(space, source_id, &accepted.publication.revision_id)
            .await?
            .context("original immutable source revision absent")?;
        let original_source = SourceRepository::new(storage.clone())
            .get(space, source_id)
            .await?
            .context("original live source absent")?;
        let expected_source = Box::pin(original_published_source(storage, accepted)).await?;
        for table in ["source_revision", "source_publication_completion"] {
            let originals =
                OriginalControlPayload::capture(storage, table, space, source_id).await?;
            for original in &originals {
                if table == "source_revision" {
                    let stored: serde_json::Value = serde_json::from_str(&original.payload)?;
                    anyhow::ensure!(
                        stored["revision"] == serde_json::to_value(&original_revision)?
                    );
                    anyhow::ensure!(stored["operation_id"] == accepted.publication.operation_id);
                } else {
                    let completion: SourcePublicationCompletion =
                        serde_json::from_str(&original.payload)?;
                    anyhow::ensure!(completion == original_completion);
                    anyhow::ensure!(original.payload == originals[0].payload);
                }
            }
            let mut immutable_completion_controls = 0;
            let mut lookup_controls = 0;
            for (index, original) in originals.iter().enumerate() {
                let mut changed: serde_json::Value = serde_json::from_str(&original.payload)?;
                if table == "source_revision" {
                    changed["revision"]["source"]["metadata"]["postcompletion_injected"] =
                        serde_json::json!("same source/revision IDs, altered immutable metadata");
                } else {
                    let fingerprints = changed["rendered_revision_fingerprints"]
                        .as_object_mut()
                        .context("original rendered revision fingerprints absent")?;
                    let fingerprint = fingerprints
                        .get_mut(&revision.base.id.to_string())
                        .context("actual revision fingerprint absent")?;
                    let altered = "0".repeat(64);
                    anyhow::ensure!(fingerprint.as_str() != Some(altered.as_str()));
                    *fingerprint = serde_json::Value::String(altered);
                }
                let changed_payload = serde_json::to_string(&changed)?;
                original.write_payload(storage, &changed_payload).await?;
                let actual = observed_future("original_control_corruption", async {
                    let mut expected_rows = originals.clone();
                    expected_rows[index] = original.with_payload(&changed_payload);
                    anyhow::ensure!(
                        OriginalControlPayload::capture(storage, table, space, source_id).await?
                            == expected_rows,
                        "exactly one original physical control must change"
                    );
                    // Identify each copy through the immutable accessor after
                    // corruption; never guess private keys or pick a first row.
                    let immutable_proof_changed = if table == "source_publication_completion" {
                        let observed = manifests
                            .completion(space, &accepted.publication)
                            .await?
                            .context("original immutable certificate disappeared")?;
                        if observed == original_completion {
                            lookup_controls += 1;
                            false
                        } else {
                            let changed_completion: SourcePublicationCompletion =
                                serde_json::from_str(&changed_payload)?;
                            anyhow::ensure!(observed == changed_completion);
                            immutable_completion_controls += 1;
                            true
                        }
                    } else {
                        true
                    };
                    let reader = ReadEligibility::new(storage.clone());
                    let doc_result =
                        observed_future("project:doc", reader.project(doc.clone(), &authorize))
                            .await?;
                    let revision_result = observed_future(
                        "project:revision",
                        reader.project(revision.clone(), &authorize),
                    )
                    .await?;
                    if immutable_proof_changed {
                        anyhow::ensure!(doc_result.is_none(), "{table}: altered proof exposed Doc");
                        anyhow::ensure!(
                            revision_result.is_none(),
                            "{table}: altered proof exposed rendered revision"
                        );
                    } else {
                        // Document authority uses the still-intact immutable
                        // certificate directly, not the Source progress lookup.
                        anyhow::ensure!(doc_result.as_ref() == Some(doc));
                        anyhow::ensure!(revision_result.as_ref() == Some(revision));
                    }
                    anyhow::ensure!(
                        observed_future(
                            "project:source",
                            reader.project(original_source.clone(), &authorize)
                        )
                        .await?
                        .is_none(),
                        "{table}: changed original proof exposed Source"
                    );
                    if let Some(definition) = definition {
                        anyhow::ensure!(
                            observed_future(
                                "project:definition",
                                reader.project(definition.clone(), &authorize)
                            )
                            .await?
                            .is_none(),
                            "{table}: changed original proof exposed definition"
                        );
                    }
                    Ok::<(), anyhow::Error>(())
                })
                .await;
                // Always restore the exact captured bytes before propagating a
                // failed assertion. No dispatch, resealing or replacement IDs.
                original.write_payload(storage, &original.payload).await?;
                anyhow::ensure!(
                    OriginalControlPayload::capture(storage, table, space, source_id).await?
                        == originals,
                    "the entire original physical row set must be restored"
                );
                actual?;
                let reader = ReadEligibility::new(storage.clone());
                anyhow::ensure!(
                    observed_future("project:doc", reader.project(doc.clone(), &authorize))
                        .await?
                        .as_ref()
                        == Some(doc),
                    "{table}: exact restoration lost original completed Doc"
                );
                anyhow::ensure!(
                    observed_future(
                        "project:revision",
                        reader.project(revision.clone(), &authorize)
                    )
                    .await?
                    .as_ref()
                        == Some(revision),
                    "{table}: exact restoration lost original completed revision"
                );
                anyhow::ensure!(
                    observed_future(
                        "project:source",
                        reader.project(original_source.clone(), &authorize)
                    )
                    .await?
                    .as_ref()
                        == Some(&expected_source),
                    "{table}: exact restoration lost original published Source"
                );
                if let Some(definition) = definition {
                    anyhow::ensure!(
                        observed_future(
                            "project:definition",
                            reader.project(definition.clone(), &authorize)
                        )
                        .await?
                        .as_ref()
                            == Some(definition),
                        "{table}: exact restoration lost original definition"
                    );
                }
            }
            if table == "source_publication_completion" {
                anyhow::ensure!(immutable_completion_controls == 1 && lookup_controls == 1);
            }
        }
        Ok(())
    }

    async fn original_render_publication(
        storage: StorageClient,
        include_definition: bool,
    ) -> anyhow::Result<()> {
        const RAW: &str = "Alice works at Acme Corp.  \r\n";
        const RENDERED: &str = "Alice works at Acme Corp.\n";
        elephant_migrations::apply_schema(&storage)
            .await
            .map_err(anyhow::Error::msg)?;
        let fixture = GroundedSourceFixture::new(&storage, "render-proof-oracle").await?;
        let extractor = EmploymentExtractor::new();
        let accepted =
            Box::pin(fixture.publish_text(&storage, "Original render input", RAW, &extractor))
                .await?;
        eprintln!(
            "render proof phase=original_publication_complete definition={include_definition}"
        );
        let accepted_doc = accepted
            .document
            .as_ref()
            .context("accepted source omitted its document")?;
        let docs = DocRepository::new(storage.clone());
        let revisions = DocRevisionRepository::new(storage.clone());
        let doc = docs
            .get(&fixture.space, &accepted_doc.base.id)
            .await?
            .context("published document absent")?;
        let original = revisions.find_by_doc(&doc.base.id).await?;
        anyhow::ensure!(
            original.len() == 1,
            "exact accepted document must have one immutable rendered revision"
        );
        let revision = original[0].clone();
        anyhow::ensure!(revision.status == RevisionStatus::Active);
        anyhow::ensure!(
            accepted_doc.text == RAW && doc.text == RENDERED && revision.text == RENDERED
                && accepted.source.manual_doc_template()?.context("accepted raw template absent")?.text == RAW,
            "the real renderer updates the Doc projection while preserving the exact accepted raw input"
        );
        anyhow::ensure!(revision.text_hash == AssertionValidator::hash_quote(RENDERED));
        let authorize = |base: &BaseFields| base.space_id == fixture.space;
        let reader = ReadEligibility::new(storage.clone());
        anyhow::ensure!(
            observed_future("project:doc", reader.project(doc.clone(), &authorize))
                .await?
                .is_some(),
            "actual finalized rendered document must be current"
        );
        anyhow::ensure!(
            observed_future(
                "project:revision",
                reader.project(revision.clone(), &authorize)
            )
            .await?
            .is_some(),
            "actual normalized immutable revision must be current"
        );

        let mut changed_text = revision.clone();
        changed_text.text = "Unsupported replacement with a matching recomputed hash.\n".into();
        changed_text.text_hash = AssertionValidator::hash_quote(&changed_text.text);
        anyhow::ensure!(
            observed_future(
                "project:changed_text",
                reader.project(changed_text, &authorize)
            )
            .await?
            .is_none(),
            "a self-consistent hash does not replace the original completed render proof"
        );
        let mut changed_renderer = revision.clone();
        changed_renderer.renderer.version.push_str("-changed");
        anyhow::ensure!(
            observed_future(
                "project:changed_renderer",
                reader.project(changed_renderer, &authorize)
            )
            .await?
            .is_none(),
            "renderer identity is part of the exact proof"
        );
        let mut changed_doc = doc.clone();
        changed_doc.title = "Changed after finalization".into();
        anyhow::ensure!(
            observed_future(
                "project:changed_doc",
                reader.project(changed_doc, &authorize)
            )
            .await?
            .is_none(),
            "all original document fields remain proof-bound"
        );

        eprintln!("render proof phase=original_read_controls_complete");
        let definition = if include_definition {
            eprintln!("render proof phase=definition_commit_begin");
            let definition = Box::pin(commit_original_definition(
                &storage,
                &fixture.space,
                &revision.base.id,
            ))
            .await?;
            anyhow::ensure!(
                observed_future(
                    "project:definition",
                    ReadEligibility::new(storage.clone()).project(definition.clone(), &authorize)
                )
                .await?
                .is_some(),
                "actual committed original definition must be current"
            );
            eprintln!("render proof phase=definition_commit_and_read_complete");
            observed_future(
                "split_source_helper",
                assert_split_source_envelope_authorization(&storage, &accepted),
            )
            .await?;
            eprintln!("render proof phase=split_source_authorization_complete");
            let sources = SourceRepository::new(storage.clone());
            let original_raw = sources
                .get(&fixture.space, &accepted.source.base.id)
                .await?
                .context("original live Source absent")?;
            let mut normalized_raw = original_raw.clone();
            normalized_raw.base.updated_at =
                original_raw.base.updated_at + chrono::Duration::seconds(1);
            normalized_raw.metadata["source_normalization_oracle"] =
                serde_json::json!({"normalized": true});
            sources
                .update(
                    &fixture.space,
                    &normalized_raw.base.id,
                    normalized_raw.clone(),
                )
                .await?;
            let prepared = Box::pin(assert_original_published_source_dependency(
                &storage,
                &definition,
                &accepted,
            ))
            .await;
            sources
                .update(&fixture.space, &original_raw.base.id, original_raw.clone())
                .await?;
            prepared?;
            Some(definition)
        } else {
            None
        };
        Box::pin(reject_changed_original_controls(
            &storage,
            &doc,
            &revision,
            &accepted,
            definition.as_ref(),
        ))
        .await?;

        eprintln!("render proof phase=original_tamper_controls_complete");
        // Accept a real new generation, deliberately before enqueue/dispatch.
        // This supplies no completed manifest or new publication authority.
        let next_text = "Alice works at Acme Corp.  \r\nNew original rendering context.\r\n";
        let mut next_source = accepted.source.clone();
        next_source.hash = AssertionValidator::hash_quote(next_text);
        let mut template = next_source
            .manual_doc_template()?
            .context("original input template absent")?;
        template.doc_id = RecordId::new_v4(TableName::Doc).to_string();
        template.title = "Pending replacement input".into();
        template.text = next_text.into();
        next_source.set_manual_doc_template(&template)?;
        let prepared = Box::pin(PreparedSourceIntake::prepare(
            &storage,
            next_source,
            IntakeControl {
                revision_id: Some("render-proof-replacement".into()),
                idempotency_key: Some("render-proof-replacement-original".into()),
                expected_generation: Some(accepted.publication.generation.clone()),
            },
        ))
        .await?;
        let next = Box::pin(prepared.accept(&storage)).await?;
        anyhow::ensure!(next.publication.generation != accepted.publication.generation);
        let publication = SourcePublicationRepository::new(storage.clone());
        let head = publication
            .head(&fixture.space, &accepted.source.base.id)
            .await?
            .context("current head absent")?;
        anyhow::ensure!(
            head.published_revision.as_deref() == Some(accepted.publication.revision_id.as_str()),
            "acceptance alone cannot replace the visible original revision"
        );
        let reader = ReadEligibility::new(storage.clone());
        anyhow::ensure!(
            observed_future("project:doc", reader.project(doc.clone(), &authorize))
                .await?
                .is_some(),
            "pending newer generation must preserve the original completed document"
        );
        anyhow::ensure!(
            observed_future(
                "project:revision",
                reader.project(revision.clone(), &authorize)
            )
            .await?
            .is_some(),
            "proof must resolve the original published generation, not latest accepted input"
        );
        let pending_doc = next.document.as_ref().context("pending document absent")?;
        anyhow::ensure!(
            observed_future(
                "project:pending_doc",
                reader.project(pending_doc.clone(), &authorize)
            )
            .await?
            .is_none(),
            "unpublished accepted document is not current evidence"
        );
        if let Some(definition) = definition.as_ref() {
            anyhow::ensure!(
                observed_future(
                    "project:definition",
                    reader.project(definition.clone(), &authorize)
                )
                .await?
                .is_some(),
                "original published R1 definition must survive desired-only R2 acceptance"
            );
            Box::pin(assert_original_published_source_dependency(
                &storage, definition, &accepted,
            ))
            .await?;
            let sources = SourceRepository::new(storage.clone());
            let original_source = sources
                .get(&fixture.space, &accepted.source.base.id)
                .await?
                .context("pending raw Source absent")?;
            let mut restricted = original_source.clone();
            restricted.base.security.level += 1;
            sources
                .update(&fixture.space, &restricted.base.id, restricted.clone())
                .await?;
            let denied = observed_future(
                "project:definition",
                ReadEligibility::new(storage.clone()).project(definition.clone(), &authorize),
            )
            .await;
            sources
                .update(
                    &fixture.space,
                    &original_source.base.id,
                    original_source.clone(),
                )
                .await?;
            anyhow::ensure!(
                denied?.is_none(),
                "stronger current Source policy cannot expose original definition"
            );
            anyhow::ensure!(
                observed_future(
                    "project:definition",
                    ReadEligibility::new(storage.clone()).project(definition.clone(), &authorize)
                )
                .await?
                .is_some(),
                "restored original Source policy must preserve pending-R2 definition read"
            );
        }

        elephant_pipeline::source_queue::enqueue_accepted_source(
            &storage,
            &next.source,
            &next.publication,
            "render-proof-oracle",
        )
        .await?;
        eprintln!("render proof phase=replacement_dispatch_begin");
        Box::pin(fixture.drive(&storage, &extractor)).await?;
        eprintln!("render proof phase=replacement_publication_complete");
        let completed = publication
            .operation(&fixture.space, &next.operation.operation_id)
            .await?
            .context("replacement operation absent")?;
        anyhow::ensure!(
            completed.state == OperationState::Completed
                && completed.published_revision.as_deref() == Some("render-proof-replacement")
        );
        let reader = ReadEligibility::new(storage.clone());
        anyhow::ensure!(
            observed_future("project:doc", reader.project(doc, &authorize))
                .await?
                .is_none()
        );
        anyhow::ensure!(
            observed_future("project:revision", reader.project(revision, &authorize))
                .await?
                .is_none(),
            "retired original revision cannot stay current after actual replacement publication"
        );
        let current_doc = docs
            .get(&fixture.space, &pending_doc.base.id)
            .await?
            .context("new published document absent")?;
        let current_revisions = revisions.find_by_doc(&current_doc.base.id).await?;
        anyhow::ensure!(current_revisions.len() == 1);
        anyhow::ensure!(
            current_revisions[0].text
                == "Alice works at Acme Corp.\nNew original rendering context.\n"
        );
        anyhow::ensure!(observed_future(
            "project:current_doc",
            reader.project(current_doc, &authorize)
        )
        .await?
        .is_some());
        anyhow::ensure!(observed_future(
            "project:current_revisions[0]",
            reader.project(current_revisions[0].clone(), &authorize)
        )
        .await?
        .is_some());
        if let Some(definition) = definition {
            anyhow::ensure!(
                observed_future("project:definition", reader.project(definition, &authorize))
                    .await?
                    .is_none(),
                "original R1 definition cannot borrow completed R2 publication"
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn embedded_actual_render_proof_preserves_original_pending_generation(
    ) -> anyhow::Result<()> {
        Box::pin(original_render_publication(
            StorageClient::new_embedded().await?,
            false,
        ))
        .await
    }

    #[tokio::test]
    async fn native_actual_render_proof_preserves_original_pending_generation() -> anyhow::Result<()>
    {
        Box::pin(original_render_publication(
            StorageClient::new_memory().await?,
            false,
        ))
        .await
    }

    #[tokio::test]
    async fn embedded_actual_definition_retains_published_source_during_pending_generation(
    ) -> anyhow::Result<()> {
        Box::pin(original_render_publication(
            StorageClient::new_embedded().await?,
            true,
        ))
        .await
    }

    #[tokio::test]
    async fn native_actual_definition_retains_published_source_during_pending_generation(
    ) -> anyhow::Result<()> {
        Box::pin(original_render_publication(
            StorageClient::new_memory().await?,
            true,
        ))
        .await
    }
}
