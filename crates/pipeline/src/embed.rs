//! Embedding generation worker.

use std::collections::{HashMap, HashSet};

use chrono::Utc;
use elephant_embeddings::{EmbeddingProvider, EmbeddingVector};
use elephant_storage::{DocNodeRepository, DocRevisionRepository, NodeEmbeddingRepository};
use elephant_types::ids::RecordId;
use elephant_types::records::doc_node::{DocNode, NodeRole};
use elephant_types::records::NodeEmbedding;
use elephant_types::Link;

use crate::error::{PipelineError, PipelineResult};

/// Sentinel `embedding_model` for nodes that can never embed (invalid span,
/// missing revision). Keeps them out of `find_pending_embedding` without
/// storing a phantom vector; null the field to send a repaired node back to
/// the pending set.
pub const UNEMBEDDABLE_SENTINEL: &str = elephant_storage::UNEMBEDDABLE_EMBEDDING_MODEL;

/// Result of an embedding operation.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct EmbedResult {
    /// Number of nodes embedded.
    pub chunks_processed: usize,
    /// IDs of embedded nodes.
    pub chunk_ids: Vec<RecordId>,
    /// Model used.
    pub model: String,
}

/// Worker that generates embeddings for document nodes.
pub struct EmbedWorker<P: EmbeddingProvider> {
    node_repo: DocNodeRepository,
    revision_repo: DocRevisionRepository,
    embedding_repo: NodeEmbeddingRepository,
    provider: P,
    source_authority:
        Option<elephant_storage::source_publication_manifest::SourceEmbeddingWriteAuthority>,
}

impl<P: EmbeddingProvider> EmbedWorker<P> {
    /// Creates a new embed worker.
    #[must_use]
    pub const fn new(
        node_repo: DocNodeRepository,
        revision_repo: DocRevisionRepository,
        embedding_repo: NodeEmbeddingRepository,
        provider: P,
    ) -> Self {
        Self {
            node_repo,
            revision_repo,
            embedding_repo,
            provider,
            source_authority: None,
        }
    }

    /// Bind source vector writes to the sealed original plan and lease captured
    /// before inference. Every batch rechecks this authority in its transaction.
    #[must_use]
    pub fn with_source_authority(
        mut self,
        authority: elephant_storage::source_publication_manifest::SourceEmbeddingWriteAuthority,
    ) -> Self {
        self.source_authority = Some(authority);
        self
    }

    /// Resolves text for a batch of nodes, fetching revision text as needed.
    ///
    /// Returns the nodes whose text resolved (paired index-for-index with
    /// `texts`) and the nodes that can never resolve (`dead`): a span start at
    /// or past the revision text, an inverted span, or a missing revision. A
    /// span that merely runs past the end is clamped and kept - the common
    /// shape is text truncated after chunking, and the surviving prefix is
    /// real content. Dead nodes MUST be taken out of the pending set by the
    /// caller: one bad node previously failed the whole batch forever and
    /// wedged the space's embedding progress behind a 30s retry loop.
    async fn resolve_texts(
        &self,
        nodes: Vec<DocNode>,
    ) -> PipelineResult<(Vec<DocNode>, Vec<String>, Vec<DocNode>)> {
        let mut revision_ids_by_space: HashMap<String, Vec<RecordId>> = HashMap::new();
        let mut seen_revisions: HashSet<RecordId> = HashSet::new();

        for node in &nodes {
            if node.text.is_some() {
                continue;
            }

            let revision_id = node.revision.id().clone();
            if seen_revisions.insert(revision_id.clone()) {
                revision_ids_by_space
                    .entry(node.base.space_id.clone())
                    .or_default()
                    .push(revision_id);
            }
        }

        let mut revision_cache: HashMap<RecordId, String> = HashMap::new();
        for (space_id, revision_ids) in revision_ids_by_space {
            let revisions = self
                .revision_repo
                .get_many(&space_id, &revision_ids)
                .await?;
            for revision in revisions {
                let _ = revision_cache.insert(revision.base.id.clone(), revision.text);
            }
        }

        let mut kept = Vec::with_capacity(nodes.len());
        let mut texts = Vec::with_capacity(nodes.len());
        let mut dead = Vec::new();

        for node in nodes {
            let text = if let Some(ref t) = node.text {
                t.clone()
            } else {
                // Slice text from the cached revision body when the node does not
                // carry inline text. Borrowing from the cache avoids cloning the
                // whole revision body for every chunk in long documents.
                let Some(rev_text) = revision_cache.get(node.revision.id()) else {
                    tracing::warn!(
                        node = %node.base.id,
                        revision = %node.revision.id(),
                        "Embed dead-letter: revision not found for node"
                    );
                    dead.push(node);
                    continue;
                };

                let start = node.span.start as usize;
                let mut end = node.span.end as usize;
                if start >= rev_text.len() || start > end {
                    tracing::warn!(
                        node = %node.base.id,
                        revision = %node.revision.id(),
                        span_start = start,
                        span_end = end,
                        text_len = rev_text.len(),
                        "Embed dead-letter: span lies outside the revision text"
                    );
                    dead.push(node);
                    continue;
                }
                if end > rev_text.len() {
                    // The revision text is shorter than the span the chunker
                    // recorded (truncated after chunking). The prefix is real
                    // content - clamp and embed it rather than wedging.
                    tracing::warn!(
                        node = %node.base.id,
                        revision = %node.revision.id(),
                        span_start = start,
                        span_end = end,
                        text_len = rev_text.len(),
                        "Embed span clamped to the revision text"
                    );
                    end = rev_text.len();
                }
                // Adjust to char boundaries to avoid panicking on multi-byte UTF-8
                let mut safe_start = start;
                while safe_start > 0 && !rev_text.is_char_boundary(safe_start) {
                    safe_start -= 1;
                }
                let mut safe_end = end;
                while safe_end < rev_text.len() && !rev_text.is_char_boundary(safe_end) {
                    safe_end += 1;
                }
                rev_text[safe_start..safe_end].to_string()
            };
            kept.push(node);
            texts.push(text);
        }

        Ok((kept, texts, dead))
    }

    /// Marks nodes that can never embed so the pending query stops returning
    /// them. The sentinel occupies `embedding_model` WITHOUT a stored vector,
    /// so search never sees phantom embeddings and a repair can retry a node
    /// by nulling the field back to pending.
    async fn dead_letter(&self, space_id: &str, nodes: Vec<DocNode>) -> PipelineResult<()> {
        if nodes.is_empty() {
            return Ok(());
        }
        let ids: Vec<_> = nodes.into_iter().map(|node| node.base.id).collect();
        self.node_repo
            .mark_unmanaged_embedding_failed(space_id, &ids, UNEMBEDDABLE_SENTINEL)
            .await?;
        Ok(())
    }

    async fn persist_embeddings(
        &self,
        space_id: &str,
        nodes: Vec<DocNode>,
        embeddings: Vec<EmbeddingVector>,
    ) -> PipelineResult<EmbedResult> {
        if nodes.len() != embeddings.len() {
            return Err(PipelineError::EmbeddingError(
                elephant_embeddings::EmbeddingError::ProviderError(format!(
                    "Embedding provider returned {} vectors for {} nodes",
                    embeddings.len(),
                    nodes.len()
                )),
            ));
        }

        let model = self.provider.model().to_string();
        let now = Utc::now();
        let mut chunk_ids = Vec::with_capacity(nodes.len());
        let mut embedding_records = Vec::with_capacity(embeddings.len());

        for (node, embedding) in nodes.into_iter().zip(embeddings.into_iter()) {
            let rid = node.base.id.clone();
            let dims = i32::try_from(embedding.len()).map_err(|_| {
                PipelineError::InvalidInput(format!(
                    "Embedding dimension count {} exceeds i32 range for node {rid}",
                    embedding.len()
                ))
            })?;

            embedding_records.push(NodeEmbedding {
                base: node.base.clone(),
                node: Link::new(rid),
                model: model.clone(),
                dims,
                vector: embedding,
            });
            chunk_ids.push(node.base.id);
        }

        if let Some(authority) = &self.source_authority {
            self.embedding_repo
                .store_source_embeddings_and_mark_nodes(
                    space_id,
                    &chunk_ids,
                    &model,
                    now,
                    embedding_records,
                    authority,
                )
                .await?;
        } else {
            self.embedding_repo
                .store_embeddings_and_mark_nodes(
                    space_id,
                    &chunk_ids,
                    &model,
                    now,
                    embedding_records,
                )
                .await?;
        }

        Ok(EmbedResult {
            chunks_processed: chunk_ids.len(),
            chunk_ids,
            model,
        })
    }

    /// Generates embeddings for nodes that don't have them.
    ///
    /// Returns the number of nodes processed.
    ///
    /// # Errors
    ///
    /// Returns an error if embedding or storage fails.
    pub async fn process_pending(
        &self,
        space_id: &str,
        batch_size: usize,
    ) -> PipelineResult<EmbedResult> {
        // Find nodes without embeddings (chunk, paragraph, summary roles)
        let nodes = self
            .node_repo
            .find_pending_embedding(space_id, batch_size)
            .await?;

        if nodes.is_empty() {
            return Ok(EmbedResult {
                chunks_processed: 0,
                chunk_ids: vec![],
                model: self.provider.model().to_string(),
            });
        }

        // Resolve text for each node; nodes that can never resolve leave the
        // pending set here so they cannot wedge the next pass.
        let (nodes, texts, dead) = self.resolve_texts(nodes).await?;
        self.dead_letter(space_id, dead).await?;
        if nodes.is_empty() {
            return Ok(EmbedResult {
                chunks_processed: 0,
                chunk_ids: vec![],
                model: self.provider.model().to_string(),
            });
        }

        // A source intake may have acquired these nodes while text was loaded.
        if self.source_authority.is_none() {
            self.node_repo
                .require_unmanaged_embedding_inputs(space_id, &nodes)
                .await?;
        }
        // Generate embeddings
        let embeddings = self.provider.embed(&texts).await?;

        self.persist_embeddings(space_id, nodes, embeddings).await
    }

    /// Embeds specific nodes by ID.
    ///
    /// # Errors
    ///
    /// Returns an error if node retrieval, embedding, or storage fails.
    pub async fn embed_chunks(
        &self,
        space_id: &str,
        chunk_ids: &[RecordId],
    ) -> PipelineResult<EmbedResult> {
        let nodes = self.node_repo.get_many(space_id, chunk_ids).await?;

        if nodes.is_empty() {
            return Ok(EmbedResult {
                chunks_processed: 0,
                chunk_ids: vec![],
                model: self.provider.model().to_string(),
            });
        }

        if self.source_authority.is_none() {
            self.node_repo
                .require_unmanaged_embedding_inputs(space_id, &nodes)
                .await?;
        }

        // Same dead-letter discipline as process_pending: an explicit chunk
        // list is no reason to let one bad node fail its batchmates.
        let (nodes, texts, dead) = self.resolve_texts(nodes).await?;
        self.dead_letter(space_id, dead).await?;
        if nodes.is_empty() {
            return Ok(EmbedResult {
                chunks_processed: 0,
                chunk_ids: vec![],
                model: self.provider.model().to_string(),
            });
        }

        // A source intake may have acquired these nodes while text was loaded.
        if self.source_authority.is_none() {
            self.node_repo
                .require_unmanaged_embedding_inputs(space_id, &nodes)
                .await?;
        }
        // Generate embeddings
        let embeddings = self.provider.embed(&texts).await?;

        self.persist_embeddings(space_id, nodes, embeddings).await
    }

    /// Embed an exact declared source-publication set. Any absent node, invalid
    /// immutable span, or missing revision fails the whole requested batch;
    /// callers cannot treat clamped text or dead-letter markers as coverage.
    pub async fn embed_exact_nodes(
        &self,
        space_id: &str,
        node_ids: &[RecordId],
    ) -> PipelineResult<EmbedResult> {
        let expected: HashSet<_> = node_ids.iter().cloned().collect();
        if expected.len() != node_ids.len() {
            return Err(PipelineError::InvalidInput(
                "Duplicate required embedding node".into(),
            ));
        }
        let nodes = self.node_repo.get_many(space_id, node_ids).await?;
        let actual: HashSet<_> = nodes.iter().map(|node| node.base.id.clone()).collect();
        if actual != expected {
            return Err(PipelineError::InvalidInput(
                "stale_input: required embedding node missing".into(),
            ));
        }
        let revisions: Vec<_> = nodes
            .iter()
            .map(|node| node.revision.id().clone())
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();
        let revisions = self.revision_repo.get_many(space_id, &revisions).await?;
        let revisions: HashMap<_, _> = revisions
            .into_iter()
            .map(|revision| (revision.base.id.clone(), revision))
            .collect();
        let mut texts = Vec::with_capacity(nodes.len());
        for node in &nodes {
            let revision = revisions.get(node.revision.id()).ok_or_else(|| {
                PipelineError::InvalidInput(
                    "stale_input: required embedding revision missing".into(),
                )
            })?;
            if let Some(authority) = &self.source_authority {
                authority.validate_embedding_input(node, revision)?;
            }
            let start = usize::try_from(node.span.start)
                .map_err(|_| PipelineError::InvalidInput("Embedding span overflow".into()))?;
            let end = usize::try_from(node.span.end)
                .map_err(|_| PipelineError::InvalidInput("Embedding span overflow".into()))?;
            let text = revision
                .text
                .get(start..end)
                .filter(|text| !text.is_empty())
                .ok_or_else(|| {
                    PipelineError::InvalidInput(
                        "stale_input: required embedding span is not exact UTF-8 evidence".into(),
                    )
                })?;
            // Only a Summary has separately generated text. Paragraph/chunk
            // evidence always comes from its exact immutable revision range.
            texts.push(if node.role == NodeRole::Summary {
                node.text
                    .clone()
                    .filter(|text| !text.is_empty())
                    .ok_or_else(|| {
                        PipelineError::InvalidInput(
                            "stale_input: captured summary text missing".into(),
                        )
                    })?
            } else {
                text.to_string()
            });
        }
        if nodes.is_empty() {
            return self.persist_embeddings(space_id, nodes, Vec::new()).await;
        }
        if self.source_authority.is_none() {
            self.node_repo
                .require_unmanaged_embedding_inputs(space_id, &nodes)
                .await?;
        }
        let vectors = self.provider.embed(&texts).await?;
        self.persist_embeddings(space_id, nodes, vectors).await
    }

    /// Generates embedding for a single text without storing.
    ///
    /// # Errors
    ///
    /// Returns an error if embedding fails.
    pub async fn embed_text(&self, text: &str) -> PipelineResult<EmbeddingVector> {
        let embeddings = self.provider.embed(&[text.to_string()]).await?;
        embeddings.into_iter().next().ok_or_else(|| {
            PipelineError::EmbeddingError(elephant_embeddings::EmbeddingError::ProviderError(
                "No embedding returned".to_string(),
            ))
        })
    }
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::unwrap_used,
        clippy::mixed_attributes_style,
        clippy::similar_names,
        clippy::type_complexity,
        clippy::float_cmp,
        clippy::const_is_empty,
        clippy::expect_used,
        clippy::panic,
        unused_imports,
        dead_code,
        clippy::too_many_lines,
        clippy::cognitive_complexity
    )]
    use super::*;
    use elephant_embeddings::{FakeEmbeddingProvider, PartitionKey};
    use elephant_storage::{Repository, StorageClient, StorageConfig};
    use elephant_types::ids::TableName;
    use elephant_types::records::base::{BaseFields, Provenance, Security, SubjectsState};
    use elephant_types::records::doc_node::{DocNode, NodeRole, TextSpan};
    use elephant_types::records::doc_revision::{DocRevision, RendererMetadata, RevisionStatus};
    use elephant_types::{Link, RecordId};
    use std::sync::atomic::{AtomicU64, Ordering};

    static TEST_DB_COUNTER: AtomicU64 = AtomicU64::new(80000);

    const SCHEMA: &str = r"
        DEFINE TABLE doc SCHEMALESS;
        DEFINE TABLE doc_node SCHEMALESS;
        DEFINE TABLE doc_revision SCHEMALESS;
    ";

    struct TestContext {
        client: StorageClient,
        worker: EmbedWorker<FakeEmbeddingProvider>,
        revision_id: RecordId,
    }

    async fn setup_test_context() -> TestContext {
        let db_id = TEST_DB_COUNTER.fetch_add(1, Ordering::SeqCst);
        let config = StorageConfig {
            database: format!("embed_worker_test_{db_id}"),
            ..StorageConfig::default()
        };
        let client = StorageClient::new(config).await.expect("create client");
        client.query(SCHEMA).await.expect("apply schema");

        let partition_manager = elephant_embeddings::PartitionManager::new();
        let node_repo = DocNodeRepository::new(client.clone());
        let revision_repo = DocRevisionRepository::new(client.clone());
        let embedding_repo =
            elephant_storage::NodeEmbeddingRepository::new(client.clone(), partition_manager);
        let provider = FakeEmbeddingProvider::new("test-model", 3072);

        // Create a revision to hold node text
        let revision_id = RecordId::new_v4(TableName::DocRevision);
        let doc_id = RecordId::new_v4(TableName::Doc);
        let revision = DocRevision {
            base: BaseFields::new(revision_id.clone(), "test_space".to_string(), "test_agent".to_string()),
            doc: Link::new(doc_id),
            text: "Test chunk text at position 0. Test chunk text at position 1. Test chunk text at position 2. Test chunk text at position 3. Test chunk text at position 4.".to_string(),
            text_hash: "hash".to_string(),
            renderer: RendererMetadata {
                name: "test".to_string(),
                version: "1.0.0".to_string(),
                config_hash: None,
            },
            manifest: None,
            supersedes: None,
            status: RevisionStatus::Active,
            rendered_at: chrono::Utc::now(),
        };
        revision_repo
            .create(revision)
            .await
            .expect("create revision");

        let worker = EmbedWorker::new(node_repo, revision_repo, embedding_repo, provider);
        TestContext {
            client,
            worker,
            revision_id,
        }
    }

    fn create_test_node(revision_id: &RecordId, position: i32) -> DocNode {
        let text = format!("Test chunk text at position {position}");
        let id = RecordId::new_v4(TableName::DocNode);
        let mut base = BaseFields::new(
            id.clone(),
            "test_space".to_string(),
            "test_agent".to_string(),
        );
        base.security = Security {
            policy_id: "default".to_string(),
            level: 0,
            labels: vec![],
            handling: vec![],
        };
        base.provenance = Provenance::default();
        base.subjects_state = SubjectsState::Complete;

        DocNode {
            base,
            revision: Link::new(revision_id.clone()),
            role: NodeRole::Paragraph,
            span: TextSpan::new(0, 0), // Will use text field
            node_key: format!("node_{position}"),
            text: Some(text),
            parent: None,
            position,
            depth: 1,

            embedding_model: None,
            collection: None,
        }
    }

    #[tokio::test]
    async fn test_process_pending_empty() {
        let ctx = setup_test_context().await;

        let result = ctx
            .worker
            .process_pending("test_space", 10)
            .await
            .expect("process");
        assert_eq!(result.chunks_processed, 0);
    }

    #[tokio::test]
    async fn test_process_pending_with_nodes() {
        let ctx = setup_test_context().await;

        // Create nodes without embeddings
        for i in 0..3 {
            let node = create_test_node(&ctx.revision_id, i);
            ctx.worker.node_repo.create(node).await.expect("create");
        }

        let result = ctx
            .worker
            .process_pending("test_space", 10)
            .await
            .expect("process");
        assert_eq!(result.chunks_processed, 3);
        assert_eq!(result.model, "test-model");

        // Verify embedding_model was set on nodes
        for id in &result.chunk_ids {
            let node = ctx
                .worker
                .node_repo
                .get("test_space", id)
                .await
                .expect("get")
                .expect("exists");
            assert_eq!(node.embedding_model.as_deref(), Some("test-model"));
        }
    }

    #[tokio::test]
    async fn test_process_pending_respects_batch_size() {
        let ctx = setup_test_context().await;

        // Create 5 nodes
        for i in 0..5 {
            let node = create_test_node(&ctx.revision_id, i);
            ctx.worker.node_repo.create(node).await.expect("create");
        }

        // Process only 2
        let result = ctx
            .worker
            .process_pending("test_space", 2)
            .await
            .expect("process");
        assert_eq!(result.chunks_processed, 2);

        // Process remaining
        let result2 = ctx
            .worker
            .process_pending("test_space", 10)
            .await
            .expect("process");
        assert_eq!(result2.chunks_processed, 3);
    }

    #[tokio::test]
    async fn test_embed_text() {
        let ctx = setup_test_context().await;

        let embedding = ctx.worker.embed_text("test query").await.expect("embed");
        assert_eq!(embedding.len(), 3072);
    }

    #[tokio::test]
    async fn test_embed_specific_nodes() {
        let ctx = setup_test_context().await;

        // Create nodes
        let mut rid_objs = Vec::new();
        for i in 0..3 {
            let node = create_test_node(&ctx.revision_id, i);
            let created = ctx.worker.node_repo.create(node).await.expect("create");
            rid_objs.push(created.base.id);
        }

        // Embed only first two
        let result = ctx
            .worker
            .embed_chunks("test_space", &rid_objs[..2])
            .await
            .expect("embed");
        assert_eq!(result.chunks_processed, 2);

        // Verify third node still has no embedding_model
        let third = ctx
            .worker
            .node_repo
            .get("test_space", &rid_objs[2])
            .await
            .expect("get")
            .expect("exists");
        assert!(third.embedding_model.is_none());
    }

    /// A node whose span lies wholly outside the revision text must be
    /// dead-lettered, not wedge every future pass: this exact shape held
    /// production embedding hostage on a 30s retry loop for two days.
    #[tokio::test]
    async fn test_process_pending_dead_letters_invalid_span() {
        let ctx = setup_test_context().await;

        let mut poison = create_test_node(&ctx.revision_id, 0);
        poison.text = None;
        poison.span = TextSpan::new(5000, 5010);
        let poison = ctx.worker.node_repo.create(poison).await.expect("create");
        let good = create_test_node(&ctx.revision_id, 1);
        let good = ctx.worker.node_repo.create(good).await.expect("create");

        let result = ctx
            .worker
            .process_pending("test_space", 10)
            .await
            .expect("process");
        assert_eq!(result.chunks_processed, 1);
        assert_eq!(result.chunk_ids, vec![good.base.id.clone()]);

        let poisoned = ctx
            .worker
            .node_repo
            .get("test_space", &poison.base.id)
            .await
            .expect("get")
            .expect("exists");
        assert_eq!(
            poisoned.embedding_model.as_deref(),
            Some(UNEMBEDDABLE_SENTINEL)
        );

        // The dead-lettered node must not be re-picked: the next pass is empty.
        let again = ctx
            .worker
            .process_pending("test_space", 10)
            .await
            .expect("process");
        assert_eq!(again.chunks_processed, 0);
    }

    /// A span that starts inside the text but runs past its end is the
    /// truncated-after-chunking shape: embed the surviving prefix.
    #[tokio::test]
    async fn test_process_pending_clamps_overlong_span() {
        let ctx = setup_test_context().await;

        let mut node = create_test_node(&ctx.revision_id, 0);
        node.text = None;
        node.span = TextSpan::new(0, 100_000);
        let node = ctx.worker.node_repo.create(node).await.expect("create");

        let result = ctx
            .worker
            .process_pending("test_space", 10)
            .await
            .expect("process");
        assert_eq!(result.chunks_processed, 1);

        let embedded = ctx
            .worker
            .node_repo
            .get("test_space", &node.base.id)
            .await
            .expect("get")
            .expect("exists");
        assert_eq!(embedded.embedding_model.as_deref(), Some("test-model"));
    }

    /// A node pointing at a revision that no longer exists can never resolve;
    /// it must be dead-lettered instead of failing its batchmates.
    #[tokio::test]
    async fn test_process_pending_dead_letters_missing_revision() {
        let ctx = setup_test_context().await;

        let orphan_revision = RecordId::new_v4(TableName::DocRevision);
        let mut orphan = create_test_node(&orphan_revision, 0);
        orphan.text = None;
        orphan.span = TextSpan::new(0, 10);
        let orphan = ctx.worker.node_repo.create(orphan).await.expect("create");
        let good = create_test_node(&ctx.revision_id, 1);
        ctx.worker.node_repo.create(good).await.expect("create");

        let result = ctx
            .worker
            .process_pending("test_space", 10)
            .await
            .expect("process");
        assert_eq!(result.chunks_processed, 1);

        let dead = ctx
            .worker
            .node_repo
            .get("test_space", &orphan.base.id)
            .await
            .expect("get")
            .expect("exists");
        assert_eq!(dead.embedding_model.as_deref(), Some(UNEMBEDDABLE_SENTINEL));
    }

    /// Test that already-embedded nodes are not re-embedded.
    /// This verifies idempotent behavior of the embed worker.
    #[tokio::test]
    async fn test_embed_idempotency() {
        let ctx = setup_test_context().await;

        // Create a node
        let node = create_test_node(&ctx.revision_id, 0);
        let created = ctx.worker.node_repo.create(node).await.expect("create");
        let node_id = created.base.id;

        // First embedding pass
        let result1 = ctx
            .worker
            .process_pending("test_space", 10)
            .await
            .expect("process");
        assert_eq!(result1.chunks_processed, 1);

        // Verify embedding_model was set
        let embedded = ctx
            .worker
            .node_repo
            .get("test_space", &node_id)
            .await
            .expect("get")
            .expect("exists");
        assert_eq!(embedded.embedding_model.as_deref(), Some("test-model"));

        // Second embedding pass - should process 0 nodes
        let result2 = ctx
            .worker
            .process_pending("test_space", 10)
            .await
            .expect("process");
        assert_eq!(
            result2.chunks_processed, 0,
            "Already-embedded nodes should not be re-embedded"
        );
    }

    /// Test `embed_chunks` with non-existent node IDs.
    #[tokio::test]
    async fn test_embed_nonexistent_nodes() {
        let ctx = setup_test_context().await;

        // Try to embed non-existent nodes
        let fake_ids = [
            RecordId::new_v4(TableName::DocNode),
            RecordId::new_v4(TableName::DocNode),
        ];
        let result = ctx
            .worker
            .embed_chunks("test_space", &fake_ids)
            .await
            .expect("embed");

        assert_eq!(
            result.chunks_processed, 0,
            "Should process 0 for non-existent nodes"
        );
        assert!(result.chunk_ids.is_empty());
    }

    #[tokio::test]
    async fn test_persist_embeddings_is_atomic_when_node_update_fails() {
        #[derive(serde::Deserialize)]
        struct CountRow {
            count: Option<i64>,
        }

        let ctx = setup_test_context().await;

        let mut nodes = Vec::new();
        for i in 0..2 {
            let node = create_test_node(&ctx.revision_id, i);
            nodes.push(ctx.worker.node_repo.create(node).await.expect("create"));
        }

        let texts: Vec<String> = nodes
            .iter()
            .map(|node| node.text.clone().expect("node text"))
            .collect();
        let embeddings = ctx.worker.provider.embed(&texts).await.expect("embed");

        let missing_id = RecordId::new_v4(TableName::DocNode);
        nodes[1].base.id = missing_id.clone();
        nodes[1].base.space_id = "test_space".to_string();

        let err = ctx
            .worker
            .persist_embeddings("test_space", nodes.clone(), embeddings)
            .await
            .expect_err("persist should fail when a node disappears");
        assert!(matches!(
            err,
            PipelineError::StorageError(elephant_storage::StorageError::Conflict(ref code))
                if code == "source_embedding_requires_original_claim"
        ));

        let first = ctx
            .worker
            .node_repo
            .get("test_space", &nodes[0].base.id)
            .await
            .expect("get first node")
            .expect("first node exists");
        assert!(first.embedding_model.is_none());

        let partition = PartitionKey::new(
            "default".to_string(),
            "L0".to_string(),
            "test-model".to_string(),
            3072,
        )
        .expect("partition key");
        let mut response = ctx
            .client
            .query(&format!(
                "SELECT count() AS count FROM {} GROUP ALL",
                partition.table_name()
            ))
            .await
            .expect("count embedding rows");
        let rows: Vec<CountRow> = response.take(0).expect("parse count rows");
        let count = rows.first().and_then(|row| row.count).unwrap_or(0);
        assert_eq!(
            count, 0,
            "embedding rows should roll back with node failure"
        );
    }
    #[tokio::test]
    #[expect(
        clippy::cloned_ref_to_slice_refs,
        reason = "Retain the original owned request slice and post-refusal coverage checks in this embedding regression."
    )]
    async fn exact_source_embedding_rejects_truncation_without_marking_coverage() {
        let ctx = setup_test_context().await;
        let repo = DocNodeRepository::new(ctx.client.clone());
        let mut node = create_test_node(&ctx.revision_id, 0);
        node.text = None;
        node.span = TextSpan::new(0, u32::MAX);
        let id = node.base.id.clone();
        repo.create(node).await.expect("node");
        assert!(ctx
            .worker
            .embed_exact_nodes("test_space", &[id.clone()])
            .await
            .is_err());
        assert!(repo
            .get("test_space", &id)
            .await
            .expect("node")
            .expect("exists")
            .embedding_model
            .is_none());
        assert!(ctx
            .worker
            .embed_exact_nodes("test_space", &[RecordId::new_v4(TableName::DocNode)])
            .await
            .is_err());
    }

    #[tokio::test]
    async fn exact_source_embeddings_use_immutable_spans_and_only_typed_summary_text() {
        struct RecordingProvider {
            seen: std::sync::Arc<std::sync::Mutex<Vec<String>>>,
        }
        #[async_trait::async_trait]
        impl EmbeddingProvider for RecordingProvider {
            fn model(&self) -> &str {
                "test-model"
            }
            fn dimensions(&self) -> usize {
                3072
            }
            async fn embed(
                &self,
                texts: &[String],
            ) -> elephant_embeddings::EmbeddingResult<Vec<EmbeddingVector>> {
                self.seen
                    .lock()
                    .expect("fixture recorder")
                    .extend_from_slice(texts);
                FakeEmbeddingProvider::new("test-model", 3072)
                    .embed(texts)
                    .await
            }
        }
        let ctx = setup_test_context().await;
        let seen = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let worker = EmbedWorker::new(
            DocNodeRepository::new(ctx.client.clone()),
            DocRevisionRepository::new(ctx.client.clone()),
            NodeEmbeddingRepository::new(
                ctx.client.clone(),
                elephant_embeddings::PartitionManager::new(),
            ),
            RecordingProvider { seen: seen.clone() },
        );
        let repository = DocNodeRepository::new(ctx.client.clone());
        let mut ids = Vec::new();
        for (index, role) in [NodeRole::Paragraph, NodeRole::Chunk, NodeRole::Summary]
            .into_iter()
            .enumerate()
        {
            let mut node =
                create_test_node(&ctx.revision_id, i32::try_from(index).expect("small index"));
            node.role = role;
            node.span = TextSpan::new(0, 30);
            node.text = Some(if role == NodeRole::Summary {
                "Captured summary.".into()
            } else {
                "Untrusted mutable copy.".into()
            });
            ids.push(node.base.id.clone());
            repository.create(node).await.expect("node");
        }
        let result = worker
            .embed_exact_nodes("test_space", &ids)
            .await
            .expect("exact batch");
        assert_eq!(result.chunks_processed, 3);
        let observed = seen.lock().expect("recorded").clone();
        assert_eq!(
            observed
                .iter()
                .filter(|text| text.as_str() == "Test chunk text at position 0.")
                .count(),
            2
        );
        assert!(observed.iter().any(|text| text == "Captured summary."));
        assert!(!observed
            .iter()
            .any(|text| text == "Untrusted mutable copy."));
        let mut missing = create_test_node(&ctx.revision_id, 3);
        missing.role = NodeRole::Summary;
        missing.span = TextSpan::new(0, 30);
        missing.text = None;
        let missing_id = missing.base.id.clone();
        repository
            .create(missing)
            .await
            .expect("missing summary node");
        assert!(worker
            .embed_exact_nodes("test_space", &[missing_id])
            .await
            .is_err());
        assert_eq!(
            seen.lock().expect("recorded").len(),
            3,
            "missing summary cannot dispatch a substituted span"
        );
    }

    #[tokio::test]
    #[expect(
        clippy::cloned_ref_to_slice_refs,
        reason = "Retain the original owned request slice and exact returned identity assertion in this embedding regression."
    )]
    async fn exact_source_embedding_reports_full_requested_set() {
        let ctx = setup_test_context().await;
        let mut node = create_test_node(&ctx.revision_id, 0);
        node.span = TextSpan::new(0, 29);
        node.text = None;
        let id = node.base.id.clone();
        DocNodeRepository::new(ctx.client.clone())
            .create(node)
            .await
            .expect("node");
        let result = ctx
            .worker
            .embed_exact_nodes("test_space", &[id.clone()])
            .await
            .expect("exact embedding");
        assert_eq!(result.chunk_ids, vec![id]);
        assert_eq!(result.chunks_processed, 1);
    }
}

#[cfg(test)]
#[path = "embed_ownership_tests.rs"]
mod ownership_tests;
