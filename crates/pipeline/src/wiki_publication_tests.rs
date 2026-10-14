//! Real compiler boundaries: an original claim cannot become fresh authority.
#![allow(clippy::expect_used)]

use super::*;
use elephant_storage::{work_attempt_repository::fence_from_claim, SpaceConfigRepository};
use elephant_types::publication::AttemptFence;
use elephant_types::records::{EntityType, SpaceConfig};
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RevokeAt {
    Never,
    Planner,
    Verifier,
}

#[derive(Debug)]
struct RevokingEditorialLlm {
    inner: super::tests::MockEditorialLlm,
    storage: StorageClient,
    original: AttemptFence,
    at: RevokeAt,
    planner_calls: AtomicUsize,
    organizer_calls: AtomicUsize,
    writer_calls: AtomicUsize,
    verifier_calls: AtomicUsize,
    writers_ready: Option<Arc<tokio::sync::Barrier>>,
}

impl RevokingEditorialLlm {
    async fn revoke(&self) -> PipelineResult<()> {
        let changed = WorkItemRepository::new(self.storage.clone())
            .update_running_for_worker(
                "space",
                &self.original.work_item_id,
                &self.original.lease_token,
                WorkItemStatus::Cancelled,
                None,
            )
            .await?;
        if changed.is_none() {
            return Err(PipelineError::InvalidState(
                "Original oracle claim was not cancelled".into(),
            ));
        }
        Ok(())
    }
}

#[async_trait]
impl WikiEditorialLlm for RevokingEditorialLlm {
    async fn plan_wiki(&self, prompt: String) -> PipelineResult<WikiPlan> {
        self.planner_calls.fetch_add(1, Ordering::SeqCst);
        let output = self.inner.plan_wiki(prompt).await?;
        if self.at == RevokeAt::Planner {
            self.revoke().await?;
        }
        Ok(output)
    }
    async fn organize_wiki(&self, prompt: String) -> PipelineResult<WikiOrganization> {
        self.organizer_calls.fetch_add(1, Ordering::SeqCst);
        self.inner.organize_wiki(prompt).await
    }
    async fn write_article(&self, prompt: String) -> PipelineResult<EditorialDraft> {
        self.writer_calls.fetch_add(1, Ordering::SeqCst);
        if let Some(barrier) = &self.writers_ready {
            let views = KnowledgeViewRepository::new(self.storage.clone())
                .list_views("space", 100)
                .await?;
            assert!(
                views
                    .iter()
                    .all(|view| view.kind != KnowledgeViewKind::Article),
                "A sibling published while another original preparation was still running"
            );
            barrier.wait().await;
        }
        self.inner.write_article(prompt).await
    }
    async fn verify_article(&self, prompt: String) -> PipelineResult<EditorialVerification> {
        self.verifier_calls.fetch_add(1, Ordering::SeqCst);
        let output = self.inner.verify_article(prompt).await?;
        if self.at == RevokeAt::Verifier {
            self.revoke().await?;
        }
        Ok(output)
    }
    fn writer_model(&self) -> &str {
        self.inner.writer_model()
    }
    fn verifier_model(&self) -> &str {
        self.inner.verifier_model()
    }
}

async fn setup(embedded: bool, at: RevokeAt) -> (StorageClient, Entity, Arc<RevokingEditorialLlm>) {
    let storage = if embedded {
        StorageClient::new_embedded().await.expect("embedded")
    } else {
        let storage = StorageClient::new_memory().await.expect("native memory");
        elephant_migrations::apply_schema(&storage)
            .await
            .expect("schema");
        storage
    };
    let base = |table| {
        BaseFields::new(
            RecordId::new_v4(table),
            "space".into(),
            "wiki-oracle".into(),
        )
    };
    SpaceConfigRepository::new(storage.clone())
        .create(SpaceConfig {
            base: base(TableName::SpaceConfig),
            embedding_provider: "unconfigured".into(),
            embedding_model: "unconfigured".into(),
            embedding_dims: 1,
            processing: None,
            purpose: None,
            skill: None,
        })
        .await
        .expect("actual named configuration");
    let mut entity = Entity::new(
        base(TableName::Entity),
        EntityType::new("person"),
        "Eve".into(),
    );
    entity.base.subjects_state = SubjectsState::Complete;
    entity.summary = Some("Eve is Noah's daughter and central to the family.".into());
    let entity = EntityRepository::new(storage.clone())
        .create(entity)
        .await
        .expect("intrinsic entity");
    let work_repo = WorkItemRepository::new(storage.clone());
    let work = work_repo
        .create(WorkItem {
            base: base(TableName::WorkItem),
            kind: RECONCILE_LIVING_WIKI_KIND.into(),
            payload: json!({}),
            status: WorkItemStatus::Runnable,
            priority: 1,
            idempotency_key: "original-wiki-claim".into(),
            attempts: 0,
            lease_expires_at: None,
            leased_by: None,
            run_after: None,
            last_error: None,
        })
        .await
        .expect("actual work");
    let claimed = work_repo
        .claim_with_lease(
            "space",
            &work.base.id,
            "wiki-oracle",
            Utc::now() + chrono::Duration::minutes(5),
        )
        .await
        .expect("claim")
        .expect("claimed");
    let original = fence_from_claim(&claimed).expect("original fence");
    let llm = Arc::new(RevokingEditorialLlm {
        inner: super::tests::MockEditorialLlm::default(),
        storage: storage.clone(),
        original,
        at,
        planner_calls: AtomicUsize::new(0),
        organizer_calls: AtomicUsize::new(0),
        writer_calls: AtomicUsize::new(0),
        verifier_calls: AtomicUsize::new(0),
        writers_ready: None,
    });
    (storage, entity, llm)
}

fn creation() -> WikiCreationConfig {
    WikiCreationConfig {
        max_articles: 1,
        publish: true,
        ..WikiCreationConfig::default()
    }
}

#[tokio::test]
async fn original_claim_survives_definition_to_article_and_gates_cached_return() {
    for embedded in [true, false] {
        let (storage, entity, llm) = setup(embedded, RevokeAt::Never).await;
        let proof = KnowledgeViewRepository::new(storage.clone())
            .prepare_managed_publication("space", Some(&llm.original), None)
            .await
            .expect("proof");
        let planner =
            WikiGraphPlanner::new(storage.clone(), llm.clone(), WikiEditorialConfig::default())
                .with_attempt(llm.original.clone());
        let created = Box::pin(planner.create_from_entities("space", vec![entity], creation()))
            .await
            .expect("plan");
        assert!(created.failures.is_empty(), "{:?}", created.failures);
        assert_eq!(created.articles.len(), 1);
        let article = &created.articles[0];
        let definition = article.view.root_record.parse().expect("definition id");
        let compiler = WikiEditorialCompiler::new(
            storage.clone(),
            llm.clone(),
            WikiEditorialConfig::default(),
        )
        .with_publication_authority(proof);
        let calls = llm.writer_calls.load(Ordering::SeqCst);
        assert!(
            Box::pin(compiler.compile("another-space", &definition, true))
                .await
                .is_err()
        );
        assert_eq!(llm.writer_calls.load(Ordering::SeqCst), calls);
        let reused = Box::pin(compiler.compile("space", &definition, true))
            .await
            .expect("current cache");
        assert!(!reused.changed);
        assert_eq!(reused.revision, article.revision);
        assert_eq!(llm.writer_calls.load(Ordering::SeqCst), calls);
        llm.revoke().await.expect("cancel actual original claim");
        assert!(Box::pin(compiler.compile("space", &definition, true))
            .await
            .is_err());
        assert_eq!(llm.writer_calls.load(Ordering::SeqCst), calls);
        let stored = KnowledgeViewRepository::new(storage)
            .get_view("space", &article.view.base.id)
            .await
            .expect("lookup")
            .expect("unchanged article");
        assert_eq!(stored, article.view);
    }
}

#[tokio::test]
async fn revocation_between_planner_and_organizer_stops_before_definitions() {
    for embedded in [true, false] {
        let (storage, entity, llm) = setup(embedded, RevokeAt::Planner).await;
        let definition = wiki_article_definition_id("space", &entity.base.id.to_string());
        let planner =
            WikiGraphPlanner::new(storage.clone(), llm.clone(), WikiEditorialConfig::default())
                .with_attempt(llm.original.clone());
        assert!(
            Box::pin(planner.create_from_entities("space", vec![entity], creation()))
                .await
                .is_err()
        );
        assert_eq!(llm.planner_calls.load(Ordering::SeqCst), 1);
        assert_eq!(llm.organizer_calls.load(Ordering::SeqCst), 0);
        assert_eq!(llm.writer_calls.load(Ordering::SeqCst), 0);
        assert!(KnowledgeViewDefinitionRepository::new(storage)
            .get("space", &definition)
            .await
            .expect("lookup")
            .is_none());
    }
}

#[tokio::test]
async fn revocation_inside_verifier_preserves_definition_but_refuses_article_transaction() {
    for embedded in [true, false] {
        let (storage, entity, llm) = setup(embedded, RevokeAt::Verifier).await;
        let definition_id = wiki_article_definition_id("space", &entity.base.id.to_string());
        let planner =
            WikiGraphPlanner::new(storage.clone(), llm.clone(), WikiEditorialConfig::default())
                .with_attempt(llm.original.clone());
        let created = Box::pin(planner.create_from_entities("space", vec![entity], creation()))
            .await
            .expect("partial progress");
        assert!(created.articles.is_empty());
        assert_eq!(created.failures.len(), 1);
        assert_eq!(llm.writer_calls.load(Ordering::SeqCst), 1);
        assert_eq!(llm.verifier_calls.load(Ordering::SeqCst), 1);
        let definition = KnowledgeViewDefinitionRepository::new(storage.clone())
            .get("space", &definition_id)
            .await
            .expect("lookup")
            .expect("committed definition preserved");
        assert!(
            elephant_storage::canonical_commit::CanonicalRepository::new(storage.clone())
                .receipt("space", &created.definition_commit_id)
                .await
                .expect("receipt")
                .is_some()
        );
        let key = format!(
            "article:{definition_id}:{}",
            security_fingerprint(&definition.base.security).expect("security")
        );
        assert!(KnowledgeViewRepository::new(storage)
            .find_by_key("space", &key)
            .await
            .expect("lookup")
            .is_none());
    }
}

#[tokio::test]
async fn siblings_prepare_in_parallel_and_publish_under_one_original_epoch() {
    for embedded in [true, false] {
        let (storage, first, mut llm) = setup(embedded, RevokeAt::Never).await;
        Arc::get_mut(&mut llm)
            .expect("unshared adapter")
            .writers_ready = Some(Arc::new(tokio::sync::Barrier::new(2)));
        let mut second = first.clone();
        second.base.id = RecordId::new_v4(TableName::Entity);
        second.display_name = "Noah".into();
        second.summary = Some("Noah is recorded as a supported household topic.".into());
        let second = EntityRepository::new(storage.clone())
            .create(second)
            .await
            .expect("second actual entity");
        let planner =
            WikiGraphPlanner::new(storage.clone(), llm.clone(), WikiEditorialConfig::default())
                .with_attempt(llm.original.clone());
        let created = tokio::time::timeout(
            std::time::Duration::from_secs(30),
            Box::pin(planner.create_from_entities(
                "space",
                vec![first, second],
                WikiCreationConfig {
                    max_articles: 2,
                    ..creation()
                },
            )),
        )
        .await
        .expect("both parallel writer boundaries must be reached")
        .expect("complete partition");
        assert!(created.failures.is_empty(), "{:?}", created.failures);
        assert_eq!(created.articles.len(), 2);
        assert_eq!(
            llm.writer_calls.load(Ordering::SeqCst),
            2,
            "no provider reruns"
        );
        assert_eq!(llm.verifier_calls.load(Ordering::SeqCst), 2);
        let views = KnowledgeViewRepository::new(storage);
        for article in created.articles {
            assert!(article.changed);
            assert_eq!(
                views
                    .get_view("space", &article.view.base.id)
                    .await
                    .expect("view"),
                Some(article.view.clone())
            );
            assert_eq!(
                views
                    .get_current_revision(&article.view)
                    .await
                    .expect("revision"),
                Some(article.revision)
            );
        }
    }
}
