//! Resumable per-space embedding profile reindex. Activation is a separate admin action.

use elephant_embeddings::PartitionManager;
use elephant_storage::{
    DocNodeRepository, DocRevisionRepository, NodeEmbeddingRepository, Repository,
    SpaceConfigRepository, StorageClient,
};
use elephant_types::provider_binding::{EmbeddingReindexPhase, ModelOperation};
use elephant_types::publication::AttemptFence;
use elephant_types::records::{doc_node::NodeRole, NodeEmbedding};
use elephant_types::{Link, RecordId};

use crate::provider_dispatch::ProviderDispatchContext;
use crate::{PipelineError, PipelineResult};

/// Durable progress follows successful vector persistence, never precedes it.
#[derive(Debug, Clone, serde::Serialize)]
pub struct ReindexProgress {
    /// Original explicitly staged target reindex identity.
    pub reindex_id: String,
    /// Durable node vectors covered by the current content generation.
    pub embedded_nodes: u64,
    /// Durable entity vectors covered after the node phase completes.
    pub embedded_entities: u64,
    /// Coverage is complete; activation still requires an administrative action.
    pub ready_to_activate: bool,
    /// This page restarted coverage because indexed content changed.
    pub restarted_for_content_change: bool,
}

/// Process one bounded page. Queue owners enqueue a successor while not ready.
/// Invalid immutable text/spans fail explicitly; they cannot count as coverage.
///
/// # Errors
/// Rejects missing/stale approvals, changed source inputs, invalid spans and
/// provider failures; durable progress never advances past a failed page.
pub async fn process_embedding_reindex(
    storage: &StorageClient,
    space_id: &str,
    batch_size: usize,
) -> PipelineResult<ReindexProgress> {
    process_embedding_reindex_inner(storage, space_id, batch_size, None, None, None).await
}

/// Process one page under the original queue claim and payload target identity.
/// A later claim cannot lend authority to an already-running model operation.
///
/// # Errors
/// Also rejects expired/superseded claims or a changed original reindex target.
pub async fn process_embedding_reindex_with_attempt(
    storage: &StorageClient,
    space_id: &str,
    batch_size: usize,
    expected_reindex_id: &str,
    attempt: &AttemptFence,
) -> PipelineResult<ReindexProgress> {
    process_embedding_reindex_inner(
        storage,
        space_id,
        batch_size,
        Some(expected_reindex_id),
        Some(attempt),
        None,
    )
    .await
}

/// Process an explicitly accepted administrative target without queue delay.
/// Every page retains the ordinary current model authorization, source reads,
/// content generation checks and guarded vector/progress transactions.
///
/// # Errors
/// Rejects a replaced configuration or reindex before provider dispatch.
pub async fn process_embedding_reindex_for_target(
    storage: &StorageClient,
    space_id: &str,
    batch_size: usize,
    expected_reindex_id: &str,
    expected_configuration_id: &RecordId,
) -> PipelineResult<ReindexProgress> {
    process_embedding_reindex_inner(
        storage,
        space_id,
        batch_size,
        Some(expected_reindex_id),
        None,
        Some(expected_configuration_id),
    )
    .await
}

async fn process_embedding_reindex_inner(
    storage: &StorageClient,
    space_id: &str,
    batch_size: usize,
    expected_reindex_id: Option<&str>,
    attempt: Option<&AttemptFence>,
    expected_configuration_id: Option<&RecordId>,
) -> PipelineResult<ReindexProgress> {
    let configs = SpaceConfigRepository::new(storage.clone());
    let mut config = configs.get_by_space_id(space_id).await?.ok_or_else(|| {
        PipelineError::ConfigError("provider_authorization: space missing".into())
    })?;
    if expected_configuration_id.is_some_and(|expected| expected != &config.base.id) {
        return Err(PipelineError::StorageError(
            elephant_storage::StorageError::Conflict(
                "original reindex configuration was replaced".into(),
            ),
        ));
    }
    let processing = config.processing.as_ref().ok_or_else(|| {
        PipelineError::ConfigError("provider_authorization: configuration missing".into())
    })?;
    let pending = processing
        .pending_reindex
        .as_ref()
        .ok_or_else(|| PipelineError::InvalidInput("no reindex pending".into()))?;
    if expected_reindex_id.is_some_and(|expected| expected != pending.id) {
        return Err(PipelineError::StorageError(
            elephant_storage::StorageError::Conflict(
                "original reindex target was superseded".into(),
            ),
        ));
    }
    if let Some(attempt) = attempt {
        elephant_storage::reindex_guard::validate_attempt(storage, &config, attempt).await?;
    }
    let restarted = pending.content_generation != processing.index_generation;
    if restarted {
        config = match attempt {
            Some(attempt) => {
                configs
                    .restart_embedding_reindex_with_attempt(&config, attempt)
                    .await?
            }
            None => configs.restart_embedding_reindex(&config).await?,
        };
    }
    let pending = config
        .processing
        .as_ref()
        .and_then(|p| p.pending_reindex.as_ref())
        .ok_or_else(|| PipelineError::InvalidInput("no reindex pending".into()))?;
    if pending.complete {
        return Ok(ReindexProgress {
            reindex_id: pending.id.clone(),
            embedded_nodes: pending.embedded_nodes,
            embedded_entities: pending.embedded_entities,
            ready_to_activate: true,
            restarted_for_content_change: restarted,
        });
    }
    let cursor: Option<RecordId> = pending
        .cursor
        .as_deref()
        .map(str::parse)
        .transpose()
        .map_err(|error| PipelineError::InvalidInput(format!("invalid reindex cursor: {error}")))?;
    let context = ProviderDispatchContext::new(configs.clone(), space_id);
    let (policy, provider, provenance) =
        context.embedding(ModelOperation::ReindexEmbedding).await?;
    if provenance.config.config_id != config.base.id.to_string()
        || config
            .processing
            .as_ref()
            .map(|processing| processing.generation)
            != Some(provenance.config.generation)
    {
        return Err(PipelineError::StorageError(
            elephant_storage::StorageError::Conflict(
                "reindex approval changed during page preparation".into(),
            ),
        ));
    }
    if pending.phase == EmbeddingReindexPhase::Entities {
        return process_entity_page(
            storage,
            &configs,
            &config,
            cursor.as_ref(),
            batch_size,
            &policy,
            provider.as_ref(),
            restarted,
            attempt,
        )
        .await;
    }
    let nodes = DocNodeRepository::new(storage.clone())
        .list(space_id, batch_size.clamp(1, 256), cursor.as_ref())
        .await?;
    let last = nodes.last().map(|node| node.base.id.to_string());
    let complete = nodes.is_empty();
    let revisions = DocRevisionRepository::new(storage.clone());
    let reader = elephant_storage::read_eligibility::ReadEligibility::new(storage.clone());
    let mut embeddings = Vec::new();
    let mut eligible = Vec::new();
    let mut texts = Vec::new();
    for node in nodes {
        if !matches!(
            node.role,
            NodeRole::Chunk | NodeRole::Paragraph | NodeRole::Summary
        ) {
            continue;
        }
        let Some(node) = reader.project(node, &|_| true).await? else {
            continue;
        };
        let revision = revisions
            .get(space_id, node.revision.id())
            .await?
            .ok_or_else(|| {
                PipelineError::InvalidInput("reindex node has no live immutable revision".into())
            })?;
        let text = if node.role == NodeRole::Summary {
            node.text
                .clone()
                .filter(|text| !text.trim().is_empty())
                .ok_or_else(|| {
                    PipelineError::InvalidInput("reindex summary has no generated text".into())
                })?
        } else {
            let start = usize::try_from(node.span.start)
                .map_err(|_| PipelineError::InvalidInput("invalid reindex span".into()))?;
            let end = usize::try_from(node.span.end)
                .map_err(|_| PipelineError::InvalidInput("invalid reindex span".into()))?;
            if start >= end {
                return Err(PipelineError::InvalidInput("empty reindex span".into()));
            }
            revision
                .text
                .get(start..end)
                .ok_or_else(|| {
                    PipelineError::InvalidInput("reindex span is not exact UTF-8 evidence".into())
                })?
                .to_string()
        };
        texts.push(text);
        eligible.push(node);
    }
    if !texts.is_empty() {
        reader.verify_current().await?;
        if let Some(attempt) = attempt {
            elephant_storage::reindex_guard::validate_attempt(storage, &config, attempt).await?;
        }
        let vectors = provider.embed(&texts).await?;
        if vectors.len() != eligible.len() {
            return Err(PipelineError::InvalidInput(
                "reindex provider returned incomplete coverage".into(),
            ));
        }
        reader.verify_current().await?;
        for (node, vector) in eligible.iter().zip(vectors) {
            let mut base = node.base.clone();
            base.provenance.config_id = Some(provenance.config.config_id.clone());
            embeddings.push(NodeEmbedding {
                base,
                node: Link::new(node.base.id.clone()),
                model: policy.model.clone(),
                dims: i32::try_from(policy.dims)
                    .map_err(|_| PipelineError::InvalidInput("dimensions overflow".into()))?,
                vector,
            });
        }
        let repository = NodeEmbeddingRepository::new(storage.clone(), PartitionManager::new());
        match attempt {
            Some(attempt) => {
                repository
                    .store_reindex_embeddings_with_attempt(&config, embeddings, attempt)
                    .await?
            }
            None => {
                repository
                    .store_reindex_embeddings(&config, embeddings)
                    .await?
            }
        }
    }
    let count = u64::try_from(eligible.len())
        .map_err(|_| PipelineError::InvalidInput("reindex count overflow".into()))?;
    reader.verify_current().await?;
    let updated = record_page(&configs, &config, last, count, complete, attempt).await?;
    let pending = updated
        .processing
        .and_then(|p| p.pending_reindex)
        .ok_or_else(|| PipelineError::InvalidInput("reindex unexpectedly disappeared".into()))?;
    Ok(ReindexProgress {
        reindex_id: pending.id,
        embedded_nodes: pending.embedded_nodes,
        embedded_entities: pending.embedded_entities,
        ready_to_activate: pending.complete,
        restarted_for_content_change: restarted,
    })
}

#[allow(clippy::too_many_arguments)]
async fn process_entity_page(
    storage: &StorageClient,
    configs: &SpaceConfigRepository,
    config: &elephant_types::records::SpaceConfig,
    cursor: Option<&RecordId>,
    batch_size: usize,
    policy: &elephant_embeddings::EmbeddingPolicy,
    provider: &dyn elephant_embeddings::EmbeddingProvider,
    restarted: bool,
    attempt: Option<&AttemptFence>,
) -> PipelineResult<ReindexProgress> {
    use elephant_storage::entity_profile_embedding_repository::{
        entity_input_hash, entity_input_text, EntityProfileEmbeddingRepository,
    };
    let entities = elephant_storage::EntityRepository::new(storage.clone())
        .list(&config.base.space_id, batch_size.clamp(1, 256), cursor)
        .await?;
    let last = entities.last().map(|entity| entity.base.id.to_string());
    let complete = entities.is_empty();
    let reader = elephant_storage::read_eligibility::ReadEligibility::new(storage.clone());
    let mut eligible = Vec::new();
    for entity in entities {
        if entity.status == elephant_types::records::entity::EntityStatus::Merged
            || elephant_storage::entity_repository::is_source_inventory_entity(&entity)
        {
            continue;
        }
        let Some(current) = reader.project(entity.clone(), &|_| true).await? else {
            continue;
        };
        if entity_input_hash(&current) != entity_input_hash(&entity) {
            return Err(PipelineError::InvalidInput(
                "reindex entity input contains obsolete support".into(),
            ));
        }
        eligible.push(entity);
    }
    let texts: Vec<_> = eligible.iter().map(entity_input_text).collect();
    if texts.iter().any(String::is_empty) {
        return Err(PipelineError::InvalidInput(
            "reindex entity has empty embedding input".into(),
        ));
    }
    if !texts.is_empty() {
        reader.verify_current().await?;
        if let Some(attempt) = attempt {
            elephant_storage::reindex_guard::validate_attempt(storage, config, attempt).await?;
        }
        let vectors = provider.embed(&texts).await?;
        if vectors.len() != eligible.len() {
            return Err(PipelineError::InvalidInput(
                "reindex provider returned incomplete coverage".into(),
            ));
        }
        reader.verify_current().await?;
        let repository = EntityProfileEmbeddingRepository::new(storage.clone());
        for (entity, vector) in eligible.iter().zip(vectors) {
            match attempt {
                Some(attempt) => {
                    repository
                        .store_with_attempt(
                            config,
                            entity,
                            &policy.model,
                            &entity_input_hash(entity),
                            vector,
                            attempt,
                        )
                        .await?
                }
                None => {
                    repository
                        .store(
                            config,
                            entity,
                            &policy.model,
                            &entity_input_hash(entity),
                            vector,
                        )
                        .await?
                }
            }
        }
    }
    reader.verify_current().await?;
    let count = u64::try_from(eligible.len())
        .map_err(|_| PipelineError::InvalidInput("entity count overflow".into()))?;
    let updated = record_page(configs, config, last, count, complete, attempt).await?;
    let pending = updated
        .processing
        .and_then(|processing| processing.pending_reindex)
        .ok_or_else(|| PipelineError::InvalidInput("reindex unexpectedly disappeared".into()))?;
    Ok(ReindexProgress {
        reindex_id: pending.id,
        embedded_nodes: pending.embedded_nodes,
        embedded_entities: pending.embedded_entities,
        ready_to_activate: pending.complete,
        restarted_for_content_change: restarted,
    })
}

async fn record_page(
    configs: &SpaceConfigRepository,
    config: &elephant_types::records::SpaceConfig,
    cursor: Option<String>,
    count: u64,
    complete: bool,
    attempt: Option<&AttemptFence>,
) -> PipelineResult<elephant_types::records::SpaceConfig> {
    Ok(match attempt {
        Some(attempt) => {
            configs
                .record_embedding_reindex_page_with_attempt(
                    config, cursor, count, complete, attempt,
                )
                .await?
        }
        None => {
            configs
                .record_embedding_reindex_page(config, cursor, count, complete)
                .await?
        }
    })
}

#[cfg(test)]
mod administrative_target_tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use elephant_storage::Repository;
    use elephant_types::records::{BaseFields, SpaceConfig};
    use elephant_types::TableName;

    #[tokio::test]
    async fn replaced_configuration_and_unstaged_target_refuse_before_provider_resolution() {
        let storage = StorageClient::new_embedded().await.unwrap();
        let id = RecordId::new_v4(TableName::SpaceConfig);
        SpaceConfigRepository::new(storage.clone())
            .create(SpaceConfig {
                base: BaseFields::new(id.clone(), "admin-reindex".into(), "fixture".into()),
                embedding_provider: "fixture".into(),
                embedding_model: "never-resolve".into(),
                embedding_dims: 3,
                processing: None,
                purpose: None,
                skill: None,
            })
            .await
            .unwrap();
        let replaced = RecordId::new_v4(TableName::SpaceConfig);
        let result = process_embedding_reindex_for_target(
            &storage,
            "admin-reindex",
            128,
            "original",
            &replaced,
        )
        .await;
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("configuration was replaced"));
        let result =
            process_embedding_reindex_for_target(&storage, "admin-reindex", 128, "original", &id)
                .await;
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("configuration missing"));
        let actual = SpaceConfigRepository::new(storage)
            .get_by_space_id("admin-reindex")
            .await
            .unwrap()
            .unwrap();
        assert!(actual.processing.is_none());
    }
}

#[cfg(test)]
#[path = "embedding_reindex_continuation_tests.rs"]
mod continuation_tests;
