mod input_revalidation_controls {
    use super::*;

    #[derive(Clone, Copy)]
    enum Mutation {
        Unrelated,
        Selected,
        Definition,
    }

    struct WritingLlm {
        inner: MockEditorialLlm,
        storage: StorageClient,
        definition: RecordId,
        writer: Option<Mutation>,
        verifier: Option<Mutation>,
    }

    #[expect(
        clippy::unwrap_used,
        reason = "Fixture lookup or mutation failure must fail at its original point in the selected-input race control."
    )]
    async fn mutate(storage: &StorageClient, definition: &RecordId, change: Mutation) {
        match change {
            Mutation::Unrelated => {
                EntityRepository::new(storage.clone())
                    .create(planning_entity("Unrelated new topic"))
                    .await
                    .expect("unrelated real canonical writer");
            }
            Mutation::Selected => {
                let definition = KnowledgeViewDefinitionRepository::new(storage.clone())
                    .get("space", definition)
                    .await
                    .unwrap()
                    .unwrap();
                let root = definition_roots(&definition).into_iter().next().unwrap();
                let repo = EntityRepository::new(storage.clone());
                let mut entity = repo.get("space", root.id()).await.unwrap().unwrap();
                entity.summary = Some("A different selected fact".into());
                entity.base.updated_at = Utc::now();
                repo.update("space", root.id(), entity).await.unwrap();
            }
            Mutation::Definition => {
                let repo = KnowledgeViewDefinitionRepository::new(storage.clone());
                let mut definition = repo.get("space", definition).await.unwrap().unwrap();
                definition.title = "Changed original editorial definition".into();
                repo.upsert(definition).await.unwrap();
            }
        }
    }

    #[async_trait]
    impl WikiEditorialLlm for WritingLlm {
        async fn plan_wiki(&self, prompt: String) -> PipelineResult<WikiPlan> {
            self.inner.plan_wiki(prompt).await
        }
        async fn organize_wiki(&self, prompt: String) -> PipelineResult<WikiOrganization> {
            self.inner.organize_wiki(prompt).await
        }
        async fn write_article(&self, prompt: String) -> PipelineResult<EditorialDraft> {
            let result = self.inner.write_article(prompt).await?;
            if let Some(change) = self.writer {
                mutate(&self.storage, &self.definition, change).await;
            }
            Ok(result)
        }
        async fn verify_article(&self, prompt: String) -> PipelineResult<EditorialVerification> {
            let result = self.inner.verify_article(prompt).await?;
            if let Some(change) = self.verifier {
                mutate(&self.storage, &self.definition, change).await;
            }
            Ok(result)
        }
        fn writer_model(&self) -> &str {
            self.inner.writer_model()
        }
        fn verifier_model(&self) -> &str {
            self.inner.verifier_model()
        }
    }

    async fn fixture(
        writer: Option<Mutation>,
        verifier: Option<Mutation>,
    ) -> (
        StorageClient,
        RecordId,
        Arc<WritingLlm>,
        WikiEditorialCompiler,
    ) {
        let storage = wiki_storage().await;
        let definition = seeded_definition(&storage).await;
        let llm = Arc::new(WritingLlm {
            inner: MockEditorialLlm::default(),
            storage: storage.clone(),
            definition: definition.clone(),
            writer,
            verifier,
        });
        let compiler = WikiEditorialCompiler::new(
            storage.clone(),
            llm.clone(),
            WikiEditorialConfig::default(),
        );
        (storage, definition, llm, compiler)
    }

    #[tokio::test]
    #[expect(
        clippy::unwrap_used,
        reason = "A failed fixture revision read must fail the original publication and single-call assertions."
    )]
    async fn standalone_wiki_recomputes_unrelated_writes_without_model_replay() {
        let (storage, definition, llm, compiler) =
            fixture(Some(Mutation::Unrelated), Some(Mutation::Unrelated)).await;
        let article = compiler
            .compile("space", &definition, true)
            .await
            .expect("identical complete selected inputs can publish");
        assert!(article.changed);
        assert_eq!(llm.inner.writer_calls.load(Ordering::SeqCst), 1);
        assert_eq!(llm.inner.verifier_calls.load(Ordering::SeqCst), 1);
        assert!(KnowledgeViewRepository::new(storage)
            .get_revision("space", &article.revision.base.id)
            .await
            .unwrap()
            .is_some());
    }

    #[tokio::test]
    #[expect(
        clippy::unwrap_used,
        reason = "A failed fixture view read must fail the original zero-publication assertion after the selected-input race."
    )]
    async fn standalone_wiki_refuses_selected_change_before_verifier() {
        let (storage, definition, llm, compiler) = fixture(Some(Mutation::Selected), None).await;
        assert!(compiler.compile("space", &definition, true).await.is_err());
        assert_eq!(llm.inner.writer_calls.load(Ordering::SeqCst), 1);
        assert_eq!(llm.inner.verifier_calls.load(Ordering::SeqCst), 0);
        assert!(KnowledgeViewRepository::new(storage)
            .list_views_for_surface(
                "space",
                KnowledgeViewSurface::Wiki,
                KnowledgeViewPublicationStatus::Published,
                10,
            )
            .await
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    #[expect(
        clippy::unwrap_used,
        reason = "A failed fixture view read must fail the original zero-publication assertion after the definition race."
    )]
    async fn standalone_wiki_refuses_definition_change_after_verifier() {
        let (storage, definition, llm, compiler) = fixture(None, Some(Mutation::Definition)).await;
        assert!(compiler.compile("space", &definition, true).await.is_err());
        assert_eq!(llm.inner.writer_calls.load(Ordering::SeqCst), 1);
        assert_eq!(llm.inner.verifier_calls.load(Ordering::SeqCst), 1);
        assert!(KnowledgeViewRepository::new(storage)
            .list_views_for_surface(
                "space",
                KnowledgeViewSurface::Wiki,
                KnowledgeViewPublicationStatus::Published,
                10,
            )
            .await
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    #[expect(
        clippy::unwrap_used,
        reason = "Fixture preparation failure must fail the original bounded-recomputation and retained-epoch assertions."
    )]
    async fn wiki_revalidation_is_bounded_and_batch_keeps_original_epoch() {
        let (storage, definition, llm, compiler) = fixture(None, None).await;
        let original =
            elephant_storage::canonical_commit::CanonicalRepository::new(storage.clone())
                .current_epoch("space")
                .await
                .unwrap();
        let inputs = compiler
            .prepare_article_inputs("space", &definition, true, &original)
            .await
            .unwrap();
        let mut standalone = WikiInputFence::new(&inputs, &original, true).unwrap();
        let mut batch = WikiInputFence::new(&inputs, &original, false).unwrap();
        for _ in 0..2 {
            mutate(&storage, &definition, Mutation::Unrelated).await;
            assert!(compiler
                .verify_article_input_fence("space", &definition, true, &mut batch)
                .await
                .is_err());
            compiler
                .verify_article_input_fence("space", &definition, true, &mut standalone)
                .await
                .expect("bounded complete recomputation");
        }
        let retained = standalone.epoch.clone();
        mutate(&storage, &definition, Mutation::Unrelated).await;
        assert!(compiler
            .verify_article_input_fence("space", &definition, true, &mut standalone)
            .await
            .is_err());
        assert_eq!(standalone.epoch, retained);
        assert_eq!(batch.epoch, original);
        assert_eq!(llm.inner.writer_calls.load(Ordering::SeqCst), 0);
        assert_eq!(llm.inner.verifier_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    #[expect(
        clippy::unwrap_used,
        reason = "Fixture preparation failure must fail the original final atomic publication-fence control."
    )]
    async fn wiki_revalidation_retains_final_atomic_publication_fence() {
        let (storage, definition, _, mut compiler) = fixture(None, None).await;
        compiler.allow_input_revalidation = true;
        let original =
            elephant_storage::canonical_commit::CanonicalRepository::new(storage.clone())
                .current_epoch("space")
                .await
                .unwrap();
        let prepared = compiler
            .prepare_at_epoch("space", &definition, true, &original)
            .await
            .unwrap();
        mutate(&storage, &definition, Mutation::Unrelated).await;
        assert!(
            KnowledgeViewRepository::new(storage)
                .publish_at_epoch(prepared.view, prepared.revision, &prepared.observed_epoch,)
                .await
                .is_err(),
            "a later writer still refuses the atomic publication"
        );
    }
}
