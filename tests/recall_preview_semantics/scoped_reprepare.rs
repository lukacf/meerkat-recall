//! Real source publication/index controls; vectors are transport inputs, not relevance evidence.
use super::{index_fixture as fixture, spy};
use anyhow::Context;
use elephant_embeddings::governed::ProviderDispatchObserver;
use elephant_embeddings::{EmbeddingError, EmbeddingResult};
use elephant_mcp::scoped_search::{search, SearchRequest};
use elephant_pipeline::provider_dispatch::{with_dispatch_credentials, with_dispatch_observer};
use elephant_storage::source_publication::{SourceMutation, SourcePublicationRepository};
use elephant_storage::{EntityRepository, Repository, SpaceConfigRepository, StorageClient};
use elephant_types::provider_binding::{ModelBindingProvenance, ModelOperation};
use elephant_types::publication::SourcePrecondition;
use elephant_types::records::base::{BaseFields, SubjectsState};
use elephant_types::records::{Entity, SourceType};
use elephant_types::{RecordId, TableName};
use serde_json::json;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};

#[derive(Debug)]
struct EpochObserver {
    storage: StorageClient,
    space: String,
    retract: Option<SourceMutation>,
    introduce: bool,
    finishes: AtomicUsize,
    started: Mutex<BTreeSet<String>>,
    measured: Mutex<BTreeSet<String>>,
    completed: Mutex<BTreeSet<String>>,
}
fn failure(error: impl std::fmt::Display) -> EmbeddingError {
    EmbeddingError::ProviderError(error.to_string())
}
#[async_trait::async_trait]
impl ProviderDispatchObserver for EpochObserver {
    async fn before_dispatch(
        &self,
        call: &str,
        provenance: &ModelBindingProvenance,
    ) -> EmbeddingResult<()> {
        if provenance.operation != ModelOperation::QueryEmbedding {
            return Ok(());
        }
        assert_eq!(provenance.config.space_id, self.space);
        self.started.lock().map_err(failure)?.insert(call.into());
        Ok(())
    }
    async fn observe_usage(
        &self,
        call: &str,
        provenance: &ModelBindingProvenance,
        input: Option<u64>,
        output: Option<u64>,
    ) -> EmbeddingResult<()> {
        if provenance.operation != ModelOperation::QueryEmbedding {
            return Ok(());
        }
        assert!(input.is_some() && output == Some(0));
        self.measured.lock().map_err(failure)?.insert(call.into());
        Ok(())
    }
    async fn dispatch_finished(
        &self,
        call: &str,
        provenance: &ModelBindingProvenance,
    ) -> EmbeddingResult<()> {
        if provenance.operation != ModelOperation::QueryEmbedding {
            return Ok(());
        }
        self.completed.lock().map_err(failure)?.insert(call.into());
        if self.finishes.fetch_add(1, Ordering::SeqCst) == 0 {
            // The actual first HTTP response and usage are complete, but search
            // has not yet rechecked its captured epoch. No fabricated provider error.
            if let Some(mutation) = &self.retract {
                SourcePublicationRepository::new(self.storage.clone())
                    .retract(mutation.clone())
                    .await
                    .map_err(failure)?;
            } else if self.introduce {
                fixture::publish(
                    &self.storage,
                    &self.space,
                    "fresh-source",
                    "fresh-r1",
                    "Incident coordinator is Noor.",
                    fixture::security("default", 0),
                    &[],
                )
                .await
                .map_err(failure)?;
            } else {
                let mut base = BaseFields::new(
                    RecordId::new_v4(TableName::Entity),
                    self.space.clone(),
                    "test".into(),
                );
                base.subjects_state = SubjectsState::Complete;
                EntityRepository::new(self.storage.clone())
                    .create(Entity::new(
                        base,
                        "person".into(),
                        "Unrelated maintenance record".into(),
                    ))
                    .await
                    .map_err(failure)?;
            }
        }
        Ok(())
    }
}
impl EpochObserver {
    fn assert_accounted(&self, expected: usize) -> anyhow::Result<()> {
        let started = self.started.lock().map_err(failure)?;
        let measured = self.measured.lock().map_err(failure)?;
        let completed = self.completed.lock().map_err(failure)?;
        anyhow::ensure!(
            started.len() == expected && *started == *measured && *started == *completed
        );
        Ok(())
    }
}

async fn published_query_race(retract: bool) -> anyhow::Result<()> {
    for backend in spy::BACKENDS {
        for mode in ["semantic", "hybrid"] {
            let storage = spy::storage(backend).await?;
            let endpoint = spy::Spy::start().await?;
            let space = spy::SPACES[0];
            spy::configure(
                &SpaceConfigRepository::new(storage.clone()),
                space,
                &endpoint.endpoint,
                8,
            )
            .await?;
            let text = "The incident coordinator is Mira.";
            let query = "incident coordinator";
            let mut vector = vec![0.0; 8];
            vector[0] = 1.0;
            endpoint.declared_vectors(BTreeMap::from([
                (query.into(), vector.clone()),
                (text.into(), vector),
            ]))?;
            fixture::publish(
                &storage,
                space,
                "exact-source",
                "original-r1",
                text,
                fixture::security("default", 0),
                &[],
            )
            .await?;
            let publication = SourcePublicationRepository::new(storage.clone());
            let head = publication
                .external_head(space, SourceType::Manual, "exact-source")
                .await?
                .context("published source")?;
            let mutation = if retract {
                Some(SourceMutation {
                    space_id: space.into(),
                    source_id: head.source_id.clone(),
                    expected: SourcePrecondition::Generation(head.generation),
                    idempotency_key: "retract-during-query".into(),
                    request_fingerprint: "retract-exact-source".into(),
                    actor: "test".into(),
                    authorized: publication
                        .authorization_snapshot(space, &head.source_id)
                        .await?
                        .context("source authority")?,
                    authorized_document: None,
                })
            } else {
                None
            };
            let observer = Arc::new(EpochObserver {
                storage: storage.clone(),
                space: space.into(),
                retract: mutation,
                introduce: false,
                finishes: AtomicUsize::new(0),
                started: Mutex::new(BTreeSet::new()),
                measured: Mutex::new(BTreeSet::new()),
                completed: Mutex::new(BTreeSet::new()),
            });
            let request: SearchRequest = serde_json::from_value(json!({
                "query":query,"ranking_mode":mode,"limit":3,
                "source_revision_constraints":{"exact-source":"original-r1"}
            }))?;
            let before = endpoint.count()?;
            let result = with_dispatch_credentials(
                Arc::new(spy::SpyCredentials),
                with_dispatch_observer(
                    observer.clone(),
                    search(&fixture::reader(&storage, space), &request),
                ),
            )
            .await;
            if retract {
                anyhow::ensure!(
                    result.is_err(),
                    "the retry must not adopt withdrawn original evidence"
                );
                anyhow::ensure!(
                    endpoint.count()? == before + 1,
                    "source change must not dispatch a second attempt"
                );
                observer.assert_accounted(1)?;
            } else {
                let response =
                    result.map_err(|error| anyhow::anyhow!("search failed: {error:?}"))?;
                anyhow::ensure!(!response.partial && response.results.len() == 1);
                anyhow::ensure!(response.coverage["exact-source"].state == "complete");
                let result = &response.results[0];
                anyhow::ensure!(result.text == text && result.evidence.len() == 1);
                let evidence = &result.evidence[0];
                anyhow::ensure!(
                    evidence.source_id == "exact-source"
                        && evidence.revision == "original-r1"
                        && evidence.quote == text
                        && evidence.span.start == 0
                        && evidence.span.end as usize == text.len()
                        && evidence.rendered_quote_sha256 == fixture::digest(text)
                );
                anyhow::ensure!(
                    endpoint.count()? == before + 2,
                    "both actual original-query calls remain visible"
                );
                observer.assert_accounted(2)?;
            }
        }
    }
    Ok(())
}
#[tokio::test]
async fn actual_index_reprepare_keeps_exact_original_evidence_and_accounts_both_calls(
) -> anyhow::Result<()> {
    published_query_race(false).await
}
#[tokio::test]
async fn actual_index_source_change_during_query_is_terminal_without_second_dispatch(
) -> anyhow::Result<()> {
    published_query_race(true).await
}

#[tokio::test]
async fn actual_hybrid_reprepare_rebuilds_lexical_candidates_after_unrelated_publication(
) -> anyhow::Result<()> {
    for backend in spy::BACKENDS {
        let storage = spy::storage(backend).await?;
        let endpoint = spy::Spy::start().await?;
        let space = spy::SPACES[0];
        spy::configure(
            &SpaceConfigRepository::new(storage.clone()),
            space,
            &endpoint.endpoint,
            8,
        )
        .await?;
        let text = "Incident coordinator is Mira.";
        let fresh = "Incident coordinator is Noor.";
        let query = "incident coordinator";
        let mut first = vec![0.0; 8];
        first[0] = 1.0;
        let mut second = vec![0.0; 8];
        second[0] = 0.8;
        second[1] = 0.6;
        endpoint.declared_vectors(BTreeMap::from([
            (query.into(), first.clone()),
            (text.into(), first),
            (fresh.into(), second),
        ]))?;
        fixture::publish(
            &storage,
            space,
            "exact-source",
            "original-r1",
            text,
            fixture::security("default", 0),
            &[],
        )
        .await?;
        let observer = Arc::new(EpochObserver {
            storage: storage.clone(),
            space: space.into(),
            retract: None,
            introduce: true,
            finishes: AtomicUsize::new(0),
            started: Mutex::new(BTreeSet::new()),
            measured: Mutex::new(BTreeSet::new()),
            completed: Mutex::new(BTreeSet::new()),
        });
        let request: SearchRequest =
            serde_json::from_value(json!({"query":query,"ranking_mode":"hybrid","limit":3}))?;
        let response = with_dispatch_credentials(
            Arc::new(spy::SpyCredentials),
            with_dispatch_observer(
                observer.clone(),
                search(&fixture::reader(&storage, space), &request),
            ),
        )
        .await
        .map_err(|error| anyhow::anyhow!("reprepare failed: {error:?}"))?;
        observer.assert_accounted(2)?;
        anyhow::ensure!(response.results.len() == 2 && !response.partial);
        let new = response
            .results
            .iter()
            .find(|entry| entry.evidence[0].source_id == "fresh-source")
            .context("newly published source missing")?;
        anyhow::ensure!(
            new.text == fresh
                && new.evidence[0].revision == "fresh-r1"
                && new.evidence[0].quote == fresh
        );
        // Semantic rank two contributes exactly 1/2. A full new lexical search
        // adds at least another 1/2; retrying only embed with the old doc_scores
        // would return 1/2. The new source was absent at first lexical assembly.
        anyhow::ensure!(
            new.score >= 1.0,
            "old lexical assembly survived reprepare: {}",
            new.score
        );
        anyhow::ensure!(response.coverage["fresh-source"].state == "complete");
    }
    Ok(())
}
