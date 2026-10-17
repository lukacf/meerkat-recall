//! `NodeEmbedding` repository implementation for `SurrealDB`.

use std::collections::{BTreeMap, HashSet};
use std::fmt::Write as _;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::Deserialize;

use elephant_embeddings::{level_bucket, PartitionKey, PartitionManager};
use elephant_types::records::entity::Entity;
use elephant_types::records::{DocNode, NodeEmbedding};
use elephant_types::{Link, RecordId};

use crate::client::StorageClient;
use crate::error::{StorageError, StorageResult};
use crate::repository::Repository;
use crate::require::required_field;

/// Lower bound on the HNSW candidate list explored per partition search.
const PARTITION_KNN_MIN_EF: usize = 100;

/// Builds the nearest-neighbour query for one embedding partition.
///
/// Every partition table carries a cosine HNSW index on `vector`
/// (`PartitionManager::create_partition_ddl`). Ranking by
/// `vector::similarity::cosine` in the projection never consults it: the
/// planner iterates the whole table and scores every vector, which on a
/// 3072-dimension production partition outlived the engine deadline and sent
/// every `search_docs` call to the lexical fallback. The KNN operator selects
/// candidates through the index, and the remaining conditions are checked
/// during the graph walk, so rows from other spaces, deleted rows, and zero
/// vectors do not consume the `limit` slots.
///
/// The KNN asks for a few more neighbours than `limit`. Rows with
/// byte-identical vectors share one graph node, and every row of an accepted
/// node takes a KNN slot even when some of those rows fail the conditions
/// and are dropped afterwards. Without the margin a partition could return
/// fewer than `limit` rows while more matches exist, which callers read as
/// "partition exhausted". The margin narrows that hole; a chunk with more
/// failing byte-identical twins than the margin can still take the slots.
fn partition_knn_query(table: &str, conditions: &[String], limit: usize) -> String {
    let knn_k = limit.saturating_add((limit / 4).max(8));
    let ef = knn_k.saturating_mul(4).max(PARTITION_KNN_MIN_EF);
    let mut predicates = Vec::with_capacity(conditions.len() + 1);
    predicates.push(format!("vector <|{knn_k},{ef}|> $vector"));
    predicates.extend(conditions.iter().cloned());
    format!(
        "SELECT node, 1 - vector::distance::knn() AS score FROM {table} \
         WHERE {} ORDER BY score DESC LIMIT {limit}",
        predicates.join(" AND ")
    )
}

/// Whether widening the semantic candidate window can still change the
/// first `needed` documents.
///
/// Every partition that filled its window returned its best chunks, so any
/// chunk not yet seen scores at most that partition's lowest returned score;
/// `full_partition_floor` is the highest such floor. Once the `needed`-th
/// visible document scores at least that floor, no unseen chunk can displace
/// it (an unseen chunk scoring exactly the floor can tie; ties are broken by
/// document id, so an exact tie at the boundary may order differently once the
/// window widens). Counting documents alone is not enough when several partitions are
/// searched: one partition's window can be filled by a few long documents
/// while a better document there is still unseen.
#[must_use]
pub fn semantic_window_settled(
    visible_docs: usize,
    needed: usize,
    nth_visible_score: Option<f64>,
    full_partition_floor: Option<f64>,
) -> bool {
    if needed == 0 {
        return true;
    }
    if visible_docs < needed {
        return false;
    }
    match (nth_visible_score, full_partition_floor) {
        (_, None) => true,
        (Some(nth), Some(floor)) => nth >= floor,
        (None, Some(_)) => false,
    }
}

/// One partition's nearest-neighbour window, as the database returned it.
///
/// `full` and `floor` describe the RAW window, before rows whose node no
/// longer resolves to a document are dropped. Deciding "full" from the
/// filtered hits would read a window holding one orphan row as exhausted and
/// let [`semantic_window_settled`] stop while better chunks remain unseen.
#[derive(Debug, Clone, Default)]
pub struct PartitionWindow {
    /// Hits that resolve to a document.
    pub hits: Vec<NodeEmbeddingVectorHit>,
    /// The database returned as many rows as were asked for.
    pub full: bool,
    /// Lowest finite score among the raw rows.
    pub floor: Option<f64>,
}

impl PartitionWindow {
    fn from_raw(hits: Vec<NodeEmbeddingVectorHit>, raw_scores: &[f64], limit: usize) -> Self {
        let floor = raw_scores
            .iter()
            .copied()
            .filter(|score| score.is_finite())
            .fold(None, |lowest: Option<f64>, score| {
                Some(lowest.map_or(score, |seen| seen.min(score)))
            });
        Self {
            hits,
            full: limit > 0 && raw_scores.len() >= limit,
            floor,
        }
    }

    /// Fold this window into the running floor across full partitions.
    #[must_use]
    pub const fn raise_floor(&self, floor: Option<f64>) -> Option<f64> {
        if !self.full {
            return floor;
        }
        match (floor, self.floor) {
            (Some(seen), Some(own)) => Some(seen.max(own)),
            (None, own) => own,
            (seen, None) => seen,
        }
    }
}

/// `NodeEmbedding` repository for `SurrealDB`.
#[derive(Clone, Debug)]
pub struct NodeEmbeddingRepository {
    client: StorageClient,
    partition_manager: PartitionManager,
}

fn embedding_record_id(embedding: &NodeEmbedding) -> RecordId {
    embedding_record_id_for(
        embedding.node.id(),
        &embedding.base.space_id,
        &embedding.base.security,
        &embedding.model,
    )
}

/// Deterministic storage key shared by staged writes and publication coverage.
#[must_use]
pub(crate) fn embedding_record_id_for(
    node: &RecordId,
    space: &str,
    security: &elephant_types::records::base::Security,
    model: &str,
) -> RecordId {
    if !model.starts_with("recall:") {
        return node.clone();
    }
    let identity = format!(
        "{}:{}:{}:{}",
        space, security.policy_id, security.level, model
    );
    RecordId::new(
        elephant_types::TableName::ChunkEmbedding,
        uuid::Uuid::new_v5(&node.id(), identity.as_bytes()),
    )
}

impl NodeEmbeddingRepository {
    /// Creates a new chunk embedding repository.
    #[must_use]
    pub const fn new(client: StorageClient, partition_manager: PartitionManager) -> Self {
        Self {
            client,
            partition_manager,
        }
    }

    fn validate_embedding_batch(
        space_id: &str,
        node_ids: &[RecordId],
        model: &str,
        embeddings: &[NodeEmbedding],
    ) -> StorageResult<()> {
        if node_ids.len() != embeddings.len() {
            return Err(StorageError::QueryError(format!(
                "Embedding batch length mismatch: {} node ids for {} embeddings",
                node_ids.len(),
                embeddings.len()
            )));
        }

        for (node_id, embedding) in node_ids.iter().zip(embeddings) {
            if embedding.node.id() != node_id {
                return Err(StorageError::QueryError(format!(
                    "Embedding batch mismatch: node id {} does not match embedding node {}",
                    node_id,
                    embedding.node.id()
                )));
            }
            if embedding.base.space_id != space_id {
                return Err(StorageError::QueryError(format!(
                    "Embedding batch space mismatch: expected `{space_id}` but embedding {} targets `{}`",
                    embedding.node.id(),
                    embedding.base.space_id
                )));
            }
            if embedding.model != model {
                return Err(StorageError::QueryError(format!(
                    "Embedding batch model mismatch: expected `{model}` but embedding {} targets `{}`",
                    embedding.node.id(),
                    embedding.model
                )));
            }
        }

        Ok(())
    }

    /// Stores an embedding in the appropriate partition.
    ///
    /// # Errors
    ///
    /// Returns an error if partition creation, query execution, or deserialization fails.
    pub async fn store_embedding(&self, embedding: NodeEmbedding) -> StorageResult<NodeEmbedding> {
        let mut stored = self.store_embeddings_bulk(vec![embedding]).await?;
        stored.pop().ok_or_else(|| {
            StorageError::QueryError(
                "Bulk embedding store returned no records for a single input".to_string(),
            )
        })
    }

    /// Stores multiple embeddings while preserving the requested order.
    ///
    /// # Errors
    ///
    /// Returns an error if partition creation, query execution, or deserialization fails.
    #[allow(clippy::too_many_lines)]
    pub async fn store_embeddings_bulk(
        &self,
        embeddings: Vec<NodeEmbedding>,
    ) -> StorageResult<Vec<NodeEmbedding>> {
        if embeddings.is_empty() {
            return Ok(Vec::new());
        }

        if let Some(store) = self.client.embedded_store() {
            return store
                .mutate(|mutation| {
                    let now = Utc::now();
                    let mut stored = Vec::with_capacity(embeddings.len());
                    for mut embedding in embeddings {
                        embedding.base.id = embedding_record_id(&embedding);
                        embedding.base.updated_at = now;
                        if mutation
                            .get::<NodeEmbedding>(
                                &embedding.base.space_id,
                                &embedding.base.id,
                                true,
                            )?
                            .is_some()
                        {
                            stored.push(mutation.replace(embedding)?);
                        } else {
                            stored.push(mutation.create(embedding)?);
                        }
                    }
                    Ok(stored)
                })
                .await;
        }

        let now = Utc::now();
        let mut stored = Vec::with_capacity(embeddings.len());
        let mut ensured_partitions = HashSet::new();

        for mut embedding in embeddings {
            let vector_norm = embedding
                .vector
                .iter()
                .map(|value| value * value)
                .sum::<f32>()
                .sqrt();
            let key = PartitionKey::new(
                embedding.base.security.policy_id.clone(),
                level_bucket(embedding.base.security.level),
                embedding.model.clone(),
                embedding.vector.len(),
            )
            .map_err(|e| StorageError::QueryError(e.to_string()))?;

            let table_name = key.table_name();
            if ensured_partitions.insert(table_name.clone()) {
                self.partition_manager
                    .ensure_partition(&key, {
                        let client = self.client.clone();
                        move |ddl| async move {
                            client
                                .query(&ddl)
                                .await
                                .map(|_| ())
                                .map_err(|e| e.to_string())
                        }
                    })
                    .await
                    .map_err(|e| StorageError::QueryError(e.to_string()))?;
            }

            let embedding_id = surrealdb::sql::Thing::from((
                table_name,
                surrealdb::sql::Id::Uuid(embedding.node.id().id().into()),
            ));

            embedding.base.updated_at = now;

            // Checked: a failed UPSERT (write conflict, dims/type error) must
            // surface instead of the embedding being reported as stored while
            // the vector was never persisted.
            self.client
                .query_bind_checked(
                    "UPSERT $id SET
                            space_id = $space_id,
                            created_at = $created_at,
                            updated_at = $updated_at,
                            created_by = $created_by,
                            deleted_at = $deleted_at,
                            security = $security,
                            provenance = $provenance,
                            subjects = $subjects,
                            subjects_state = $subjects_state,
                            node = $node,
                            model = $model,
                            dims = type::int($dims),
                            vector_norm = $vector_norm,
                            vector = $vector;
                        SELECT * FROM $id",
                    crate::bindings! {
                        "id" => embedding_id,
                        "space_id" => embedding.base.space_id.clone(),
                        "created_at" => crate::surreal::datetime_from_chrono(embedding.base.created_at),
                        "updated_at" => crate::surreal::datetime_from_chrono(now),
                        "created_by" => embedding.base.created_by.clone(),
                        "deleted_at" => embedding.base.deleted_at.map_or(surrealdb::sql::Value::None, |value| {
                            surrealdb::sql::Value::from(crate::surreal::datetime_from_chrono(value))
                        }),
                        "security" => crate::surreal::security_to_value(&embedding.base.security),
                        "provenance" => crate::surreal::provenance_to_value(&embedding.base.provenance)?,
                        "subjects" => crate::surreal::things_from_links(embedding.base.subjects.clone()),
                        "subjects_state" => crate::surreal::enum_to_surreal(&embedding.base.subjects_state)?,
                        "node" => crate::surreal::thing_from_record_id(embedding.node.id()),
                        "model" => embedding.model.clone(),
                        "dims" => embedding.dims,
                        "vector_norm" => f64::from(vector_norm),
                        "vector" => surrealdb::sql::Value::Array(surrealdb::sql::Array::from(embedding.vector.clone())),
                    },
                )
                .await?;

            // Only reached when the checked UPSERT succeeded. Avoid
            // deserializing the stored embedding record here; the driver has
            // issues parsing some Surreal values for embedding partitions.
            stored.push(embedding);
        }

        Ok(stored)
    }

    /// Stores embeddings and marks the corresponding nodes as embedded in one
    /// logical write path.
    ///
    /// # Errors
    ///
    /// Returns an error if any node is missing/deleted, partition creation
    /// fails, or either backend cannot commit the combined write.
    #[allow(clippy::too_many_lines)]
    pub async fn store_embeddings_and_mark_nodes(
        &self,
        space_id: &str,
        node_ids: &[RecordId],
        model: &str,
        updated_at: DateTime<Utc>,
        embeddings: Vec<NodeEmbedding>,
    ) -> StorageResult<()> {
        if embeddings.is_empty() {
            return Ok(());
        }

        self.store_embeddings_transaction(
            space_id, node_ids, model, updated_at, embeddings, None, None, None,
        )
        .await
    }

    /// Persist active source-ingest vectors and node markers only under the
    /// sealed manifest and original queue claim captured before inference.
    ///
    /// # Errors
    /// Rejects superseded claims, changed source/configuration/manifest identity,
    /// undeclared nodes, or vectors that do not match their captured partitions.
    #[allow(clippy::too_many_arguments)]
    pub async fn store_source_embeddings_and_mark_nodes(
        &self,
        space_id: &str,
        node_ids: &[RecordId],
        model: &str,
        updated_at: DateTime<Utc>,
        embeddings: Vec<NodeEmbedding>,
        authority: &crate::source_publication_manifest::SourceEmbeddingWriteAuthority,
    ) -> StorageResult<()> {
        if authority.space_id() != space_id {
            return Err(StorageError::Conflict(
                "source embedding belongs to another space".into(),
            ));
        }
        // A sibling publication join may record pending dependencies while
        // inference or partition preparation runs. A refused atomic commit can
        // reconcile proven live progress without repeating inference. Every
        // attempt retains the original whole-payload and authority fences.
        authority
            .commit_with_source_progress(&self.client, |authority| {
                let embeddings = embeddings.clone();
                async move {
                    self.store_embeddings_transaction(
                        space_id,
                        node_ids,
                        model,
                        updated_at,
                        embeddings,
                        None,
                        None,
                        Some(&authority),
                    )
                    .await
                }
            })
            .await
            .map(|_| ())
    }

    // Deterministic tests inject sibling progress after the retry runner has
    // reconciled and then enter this same atomic write path without reconciling
    // a second time. This entry point is absent from production builds.
    #[cfg(test)]
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn store_source_embedding_transaction_for_test(
        &self,
        space_id: &str,
        node_ids: &[RecordId],
        model: &str,
        updated_at: DateTime<Utc>,
        embeddings: Vec<NodeEmbedding>,
        authority: &crate::source_publication_manifest::SourceEmbeddingWriteAuthority,
    ) -> StorageResult<()> {
        self.store_embeddings_transaction(
            space_id,
            node_ids,
            model,
            updated_at,
            embeddings,
            None,
            None,
            Some(authority),
        )
        .await
    }

    /// Store target vectors without changing active node markers. Current content
    /// epoch and pending profile are checked in the same transaction as the rows.
    ///
    /// # Errors
    /// Rejects stale configuration, missing nodes or invalid target vectors.
    pub async fn store_reindex_embeddings(
        &self,
        observed: &elephant_types::records::SpaceConfig,
        embeddings: Vec<NodeEmbedding>,
    ) -> StorageResult<()> {
        self.store_reindex_embeddings_inner(observed, embeddings, None)
            .await
    }

    /// Persist staged vectors only while the original reindex claim remains valid.
    ///
    /// # Errors
    /// Also rejects expired, superseded or differently targeted queue claims.
    pub async fn store_reindex_embeddings_with_attempt(
        &self,
        observed: &elephant_types::records::SpaceConfig,
        embeddings: Vec<NodeEmbedding>,
        attempt: &elephant_types::publication::AttemptFence,
    ) -> StorageResult<()> {
        self.store_reindex_embeddings_inner(observed, embeddings, Some(attempt))
            .await
    }

    async fn store_reindex_embeddings_inner(
        &self,
        observed: &elephant_types::records::SpaceConfig,
        embeddings: Vec<NodeEmbedding>,
        attempt: Option<&elephant_types::publication::AttemptFence>,
    ) -> StorageResult<()> {
        let Some(first) = embeddings.first() else {
            return Ok(());
        };
        let model = first.model.clone();
        let processing = observed
            .processing
            .clone()
            .ok_or_else(|| StorageError::Conflict("reindex processing missing".into()))?;
        let resolved = elephant_types::provider_binding::ResolvedSpaceConfig {
            token: elephant_types::provider_binding::SpaceConfigToken {
                space_id: observed.base.space_id.clone(),
                config_id: observed.base.id.to_string(),
                generation: processing.generation,
                policy_id: processing.policy_id.clone(),
            },
            processing,
            purpose: observed.purpose.clone(),
            skill: observed.skill.clone(),
        };
        let binding = resolved
            .authorize(
                &observed.base.space_id,
                elephant_types::provider_binding::ModelOperation::ReindexEmbedding,
                None,
            )
            .map_err(|error| StorageError::Conflict(error.to_string()))?;
        let policy = elephant_embeddings::governed::policy_for_binding(&binding)
            .map_err(|error| StorageError::Conflict(error.to_string()))?;
        if model != policy.model
            || embeddings.iter().any(|embedding| {
                embedding.vector.len() != policy.dims
                    || embedding.vector.iter().any(|value| !value.is_finite())
            })
        {
            return Err(StorageError::Conflict(
                "reindex vectors do not match target profile".into(),
            ));
        }
        let ids: Vec<_> = embeddings
            .iter()
            .map(|embedding| embedding.node.id().clone())
            .collect();
        self.store_embeddings_transaction(
            &observed.base.space_id,
            &ids,
            &model,
            Utc::now(),
            embeddings,
            Some(observed),
            attempt,
            None,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    #[allow(
        clippy::too_many_lines,
        reason = "Keep source, reindex and unmanaged guards adjacent to all atomic vector/node writes on both backends."
    )]
    async fn store_embeddings_transaction(
        &self,
        space_id: &str,
        node_ids: &[RecordId],
        model: &str,
        updated_at: DateTime<Utc>,
        embeddings: Vec<NodeEmbedding>,
        reindex: Option<&elephant_types::records::SpaceConfig>,
        attempt: Option<&elephant_types::publication::AttemptFence>,
        source: Option<&crate::source_publication_manifest::SourceEmbeddingWriteAuthority>,
    ) -> StorageResult<()> {
        Self::validate_embedding_batch(space_id, node_ids, model, &embeddings)?;
        let unmanaged = if source.is_none() && reindex.is_none() {
            crate::unmanaged_embedding::prepare(&self.client, space_id, node_ids).await?
        } else {
            Vec::new()
        };

        if let Some(store) = self.client.embedded_store() {
            let space_id = space_id.to_string();
            let node_ids = node_ids.to_vec();
            let model = model.to_string();
            return store
                .mutate(|mutation| {
                    crate::unmanaged_embedding::check_tx(mutation, &space_id, &unmanaged)?;
                    if let Some(source) = source {
                        crate::source_publication_manifest::check_embedding_write_tx(
                            mutation,
                            source,
                            &node_ids,
                            &model,
                            &embeddings,
                        )?;
                    }
                    if let Some(observed) = reindex {
                        if let Some(attempt) = attempt {
                            crate::reindex_guard::check(mutation, observed, attempt)?;
                        }
                        let current = mutation
                            .get::<elephant_types::records::SpaceConfig>(
                                space_id.as_str(),
                                &observed.base.id,
                                false,
                            )?
                            .ok_or_else(|| {
                                StorageError::Conflict("reindex space removed".into())
                            })?;
                        if current.processing != observed.processing {
                            return Err(StorageError::Conflict(
                                "reindex configuration/content changed".into(),
                            ));
                        }
                    }

                    for (node_id, mut embedding) in node_ids.into_iter().zip(embeddings) {
                        let Some(mut node) = mutation.get::<DocNode>(&space_id, &node_id, true)?
                        else {
                            return Err(StorageError::NotFound(format!(
                                "DocNode not found in space: {node_id}"
                            )));
                        };
                        if node.base.deleted_at.is_some() {
                            return Err(StorageError::NotFound(format!(
                                "DocNode is deleted: {node_id}"
                            )));
                        }

                        if reindex.is_none() {
                            node.embedding_model = Some(model.clone());
                            node.base.updated_at = updated_at;
                            let _ = mutation.replace(node)?;
                        }

                        embedding.base.id = embedding_record_id(&embedding);
                        embedding.base.updated_at = updated_at;
                        if mutation
                            .get::<NodeEmbedding>(
                                &embedding.base.space_id,
                                &embedding.base.id,
                                true,
                            )?
                            .is_some()
                        {
                            let _ = mutation.replace(embedding)?;
                        } else {
                            let _ = mutation.create(embedding)?;
                        }
                    }
                    Ok(())
                })
                .await;
        }

        let mut sql = String::from("BEGIN TRANSACTION;\n");
        let mut bindings = BTreeMap::<String, surrealdb::sql::Value>::new();
        crate::unmanaged_embedding::append_sql(&mut sql, &mut bindings, space_id, &unmanaged);
        if let Some(source) = source {
            crate::source_publication_manifest::append_embedding_write_sql(
                &mut sql,
                &mut bindings,
                source,
                node_ids,
                model,
                &embeddings,
            )?;
        }
        let mut ensured_partitions = HashSet::new();
        let mut prepared = Vec::with_capacity(embeddings.len());

        for mut embedding in embeddings {
            let vector_norm = embedding
                .vector
                .iter()
                .map(|value| value * value)
                .sum::<f32>()
                .sqrt();
            let key = PartitionKey::new(
                embedding.base.security.policy_id.clone(),
                level_bucket(embedding.base.security.level),
                embedding.model.clone(),
                embedding.vector.len(),
            )
            .map_err(|e| StorageError::QueryError(e.to_string()))?;

            let table_name = key.table_name();
            if ensured_partitions.insert(table_name.clone()) {
                self.partition_manager
                    .ensure_partition(&key, {
                        let client = self.client.clone();
                        move |ddl| async move {
                            client
                                .query(&ddl)
                                .await
                                .map(|_| ())
                                .map_err(|e| e.to_string())
                        }
                    })
                    .await
                    .map_err(|e| StorageError::QueryError(e.to_string()))?;
            }

            let embedding_id = surrealdb::sql::Thing::from((
                table_name,
                surrealdb::sql::Id::Uuid(embedding.node.id().id().into()),
            ));
            embedding.base.id = embedding_record_id(&embedding);
            embedding.base.updated_at = updated_at;
            prepared.push((embedding, embedding_id, f64::from(vector_norm)));
        }

        bindings.insert(
            "space_id".to_string(),
            surrealdb::sql::Value::from(space_id.to_string()),
        );
        bindings.insert(
            "model".to_string(),
            surrealdb::sql::Value::from(model.to_string()),
        );
        bindings.insert(
            "updated_at".to_string(),
            surrealdb::sql::Value::from(crate::surreal::datetime_from_chrono(updated_at)),
        );

        if let Some(observed) = reindex {
            if let Some(attempt) = attempt {
                crate::reindex_guard::append(&mut sql, &mut bindings, observed, attempt)?;
            }
            let processing = observed
                .processing
                .as_ref()
                .ok_or_else(|| StorageError::Conflict("reindex configuration missing".into()))?;
            bindings.insert(
                "reindex_config_id".into(),
                crate::surreal::thing_from_record_id(&observed.base.id).into(),
            );
            bindings.insert(
                "reindex_processing".into(),
                crate::surreal::stored_native_value(crate::surreal::json_to_surreal(
                    serde_json::to_value(processing)
                        .map_err(|error| StorageError::SerializationError(error.to_string()))?,
                )?),
            );
            // Dynamic emb__ partitions have no canonical-predicate event. A
            // no-op config UPDATE can skip the native KV write, so it cannot
            // fence concurrent approval/content changes. Share the existing
            // epoch writer used by real space-config changes in migration 031.
            sql.push_str("LET $reindex_guard = (SELECT * FROM ONLY $reindex_config_id WHERE space_id = $space_id AND processing = $reindex_processing AND deleted_at IS NONE);\nIF $reindex_guard = NONE { THROW 'reindex configuration/content changed'; };\nLET $reindex_epoch_guard = (UPDATE ONLY type::thing('canonical_epoch', $space_id) SET epoch = <string>rand::uuid() WHERE space_id = $space_id RETURN AFTER);\nIF $reindex_epoch_guard = NONE { THROW 'reindex canonical epoch missing'; };\n");
        }

        for (idx, (node_id, (embedding, embedding_id, vector_norm))) in
            node_ids.iter().zip(prepared.into_iter()).enumerate()
        {
            let prefix = format!("embedding_{idx}");
            let node_id_key = format!("node_id_{idx}");
            let updated_key = format!("updated_node_{idx}");
            if reindex.is_some() {
                let _ = writeln!(
                    sql,
                    "LET ${updated_key} = (SELECT * FROM ONLY ${node_id_key});"
                );
            } else {
                let _ = writeln!(sql, "LET ${updated_key} = (UPDATE ONLY ${node_id_key} MERGE {{ embedding_model: $model, updated_at: $updated_at }} RETURN AFTER);");
            }
            let _ = writeln!(
                sql,
                "IF ${updated_key} = NONE OR ${updated_key}.space_id != $space_id OR ${updated_key}.deleted_at != NONE {{ THROW 'DocNode not found in space: {node_id}'; }};"
            );
            let _ = writeln!(
                sql,
                "UPSERT ${prefix}_id SET
                        space_id = ${prefix}_space_id,
                        created_at = ${prefix}_created_at,
                        updated_at = $updated_at,
                        created_by = ${prefix}_created_by,
                        deleted_at = ${prefix}_deleted_at,
                        security = ${prefix}_security,
                        provenance = ${prefix}_provenance,
                        subjects = ${prefix}_subjects,
                        subjects_state = ${prefix}_subjects_state,
                        node = ${prefix}_node,
                        model = ${prefix}_model,
                        dims = type::int(${prefix}_dims),
                        vector_norm = ${prefix}_vector_norm,
                        vector = ${prefix}_vector;"
            );

            bindings.insert(
                node_id_key,
                surrealdb::sql::Value::from(crate::surreal::thing_from_record_id(node_id)),
            );
            bindings.insert(format!("{prefix}_id"), embedding_id.into());
            bindings.insert(
                format!("{prefix}_space_id"),
                surrealdb::sql::Value::from(embedding.base.space_id.clone()),
            );
            bindings.insert(
                format!("{prefix}_created_at"),
                surrealdb::sql::Value::from(crate::surreal::datetime_from_chrono(
                    embedding.base.created_at,
                )),
            );
            bindings.insert(
                format!("{prefix}_created_by"),
                surrealdb::sql::Value::from(embedding.base.created_by.clone()),
            );
            bindings.insert(
                format!("{prefix}_deleted_at"),
                embedding
                    .base
                    .deleted_at
                    .map_or(surrealdb::sql::Value::None, |value| {
                        surrealdb::sql::Value::from(crate::surreal::datetime_from_chrono(value))
                    }),
            );
            bindings.insert(
                format!("{prefix}_security"),
                crate::surreal::security_to_value(&embedding.base.security),
            );
            bindings.insert(
                format!("{prefix}_provenance"),
                crate::surreal::provenance_to_value(&embedding.base.provenance)?,
            );
            bindings.insert(
                format!("{prefix}_subjects"),
                surrealdb::sql::Value::Array(crate::surreal::things_from_links(
                    embedding.base.subjects.clone(),
                )),
            );
            bindings.insert(
                format!("{prefix}_subjects_state"),
                crate::surreal::enum_to_surreal(&embedding.base.subjects_state)?,
            );
            bindings.insert(
                format!("{prefix}_node"),
                surrealdb::sql::Value::from(crate::surreal::thing_from_record_id(
                    embedding.node.id(),
                )),
            );
            bindings.insert(
                format!("{prefix}_model"),
                surrealdb::sql::Value::from(embedding.model.clone()),
            );
            bindings.insert(
                format!("{prefix}_dims"),
                surrealdb::sql::Value::from(embedding.dims),
            );
            bindings.insert(
                format!("{prefix}_vector_norm"),
                surrealdb::sql::Value::from(vector_norm),
            );
            bindings.insert(
                format!("{prefix}_vector"),
                surrealdb::sql::Value::Array(surrealdb::sql::Array::from(embedding.vector.clone())),
            );
        }

        sql.push_str("COMMIT TRANSACTION;");
        // Checked + retried: the explicit BEGIN..COMMIT batch is atomic, so a
        // retryable conflict means nothing was committed and replaying the
        // whole transaction is safe.
        let _response = self
            .client
            .query_bind_checked(&sql, bindings)
            .await
            .map_err(|e| match e {
                StorageError::QueryError(msg) => {
                    StorageError::QueryError(format!("DocNode embedding transaction failed: {msg}"))
                }
                other => other,
            })?;
        Ok(())
    }

    /// Searches a specific embedding partition using vector similarity.
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails or deserialization fails.
    pub async fn search_partition(
        &self,
        key: &PartitionKey,
        space_id: &str,
        vector: &[f32],
        limit: usize,
        collection: Option<&str>,
        include_deleted: bool,
    ) -> StorageResult<Vec<NodeEmbeddingVectorHit>> {
        self.search_partition_window(key, space_id, vector, limit, collection, include_deleted)
            .await
            .map(|window| window.hits)
    }

    /// Like [`Self::search_partition`], also reporting whether the raw window
    /// was full and its lowest score (see [`PartitionWindow`]).
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails or deserialization fails.
    pub async fn search_partition_window(
        &self,
        key: &PartitionKey,
        space_id: &str,
        vector: &[f32],
        limit: usize,
        collection: Option<&str>,
        include_deleted: bool,
    ) -> StorageResult<PartitionWindow> {
        if let Some(store) = self.client.embedded_store() {
            return store
                .search_node_embeddings(
                    &key.table_name(),
                    space_id,
                    vector,
                    limit,
                    collection,
                    include_deleted,
                )
                .await
                .map(|hits| {
                    let raw_scores: Vec<f64> = hits.iter().map(|hit| hit.score).collect();
                    let hits = hits
                        .into_iter()
                        .map(|hit| NodeEmbeddingVectorHit {
                            node_id: hit.node_id,
                            doc_id: hit.doc_id,
                            score: hit.score,
                        })
                        .collect();
                    PartitionWindow::from_raw(hits, &raw_scores, limit)
                });
        }

        let table = key.table_name();

        let mut conditions = Vec::new();
        conditions.push("space_id = $space_id".to_string());
        if !include_deleted {
            conditions.push("deleted_at IS NONE".to_string());
        }
        if collection.is_some() {
            conditions.push("node.collection = $collection".to_string());
        }
        conditions.push("vector_norm > 0".to_string());

        let query = partition_knn_query(&table, &conditions, limit);

        let mut response = self
            .client
            .query_bind(
                &query,
                crate::bindings! {
                    "space_id" => space_id,
                    "collection" => collection.map_or(surrealdb::sql::Value::None, |value| surrealdb::sql::Value::from(value.to_string())),
                    "vector" => surrealdb::sql::Value::Array(surrealdb::sql::Array::from(vector.to_vec()))
                },
            )
            .await?;

        let hits: Vec<NodeEmbeddingVectorNodeHitDb> = response.take(0).map_err(|e| {
            StorageError::QueryError(format!("Failed to parse embedding hits: {e}"))
        })?;

        let node_ids = hits
            .iter()
            .map(|hit| crate::surreal::record_id_from_thing(&hit.node))
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| StorageError::QueryError(e.to_string()))?;
        let doc_ids_by_node = self.resolve_doc_ids_for_nodes(&node_ids).await?;
        let raw_scores: Vec<f64> = hits.iter().map(|hit| hit.score).collect();

        let mut resolved = Vec::with_capacity(hits.len());
        for hit in hits {
            if !hit.score.is_finite() {
                continue;
            }
            let node_hit = NodeEmbeddingVectorNodeHit::try_from(hit)?;
            let Some(doc_id) = doc_ids_by_node.get(&node_hit.node_id).cloned() else {
                continue;
            };
            resolved.push(NodeEmbeddingVectorHit {
                node_id: node_hit.node_id,
                doc_id,
                score: node_hit.score,
            });
        }

        Ok(PartitionWindow::from_raw(resolved, &raw_scores, limit))
    }

    async fn resolve_doc_ids_for_nodes(
        &self,
        node_ids: &[RecordId],
    ) -> StorageResult<std::collections::HashMap<RecordId, RecordId>> {
        if node_ids.is_empty() {
            return Ok(std::collections::HashMap::new());
        }

        // FROM $ids fetches the hit nodes directly. `FROM doc_node WHERE id
        // INSIDE $ids` scanned the whole table and timed out under load, which
        // silently sent semantic search to the text fallback.
        let mut node_response = self
            .client
            .query_bind(
                NODE_REVISION_LOOKUP,
                crate::bindings! {
                    "ids" => surrealdb::sql::Value::Array(surrealdb::sql::Array::from(
                        node_ids
                            .iter()
                            .map(crate::surreal::thing_from_record_id)
                            .map(surrealdb::sql::Value::from)
                            .collect::<Vec<_>>(),
                    )),
                },
            )
            .await?;

        let node_rows: Vec<NodeRevisionLookupDb> = node_response.take(0).map_err(|e| {
            StorageError::QueryError(format!("Failed to parse doc_node revision lookup: {e}"))
        })?;

        let mut revision_by_node = std::collections::HashMap::with_capacity(node_rows.len());
        let mut revision_things = Vec::with_capacity(node_rows.len());
        for row in node_rows {
            let node_id = crate::surreal::record_id_from_thing(&row.id)
                .map_err(|e| StorageError::QueryError(e.to_string()))?;
            let Some(revision) = row.revision else {
                continue;
            };
            let revision_id = crate::surreal::record_id_from_thing(&revision)
                .map_err(|e| StorageError::QueryError(e.to_string()))?;
            revision_things.push(revision);
            let _ = revision_by_node.insert(node_id, revision_id);
        }

        if revision_things.is_empty() {
            return Ok(std::collections::HashMap::new());
        }

        let mut revision_response = self
            .client
            .query_bind(
                REVISION_DOC_LOOKUP,
                crate::bindings! {
                    "ids" => surrealdb::sql::Value::Array(surrealdb::sql::Array::from(
                        revision_things
                            .into_iter()
                            .map(surrealdb::sql::Value::from)
                            .collect::<Vec<_>>(),
                    )),
                },
            )
            .await?;

        let revision_rows: Vec<RevisionDocLookupDb> = revision_response.take(0).map_err(|e| {
            StorageError::QueryError(format!("Failed to parse doc_revision doc lookup: {e}"))
        })?;
        let mut doc_by_revision = std::collections::HashMap::with_capacity(revision_rows.len());
        for row in revision_rows {
            let revision_id = crate::surreal::record_id_from_thing(&row.id)
                .map_err(|e| StorageError::QueryError(e.to_string()))?;
            let Some(doc) = row.doc else {
                continue;
            };
            let doc_id = crate::surreal::record_id_from_thing(&doc)
                .map_err(|e| StorageError::QueryError(e.to_string()))?;
            let _ = doc_by_revision.insert(revision_id, doc_id);
        }

        let mut docs_by_node = std::collections::HashMap::with_capacity(revision_by_node.len());
        for (node_id, revision_id) in revision_by_node {
            if let Some(doc_id) = doc_by_revision.get(&revision_id) {
                let _ = docs_by_node.insert(node_id, doc_id.clone());
            }
        }

        Ok(docs_by_node)
    }
}

/// Revision of each hit node, fetched by record id.
const NODE_REVISION_LOOKUP: &str = "SELECT id, revision FROM $ids";

/// Doc of each revision, fetched by record id.
const REVISION_DOC_LOOKUP: &str = "SELECT id, doc FROM $ids";

/// Vector search hit from a chunk embedding partition.
#[derive(Debug, Clone)]
pub struct NodeEmbeddingVectorHit {
    /// Chunk record ID.
    pub node_id: RecordId,
    /// Parent document ID.
    pub doc_id: RecordId,
    /// Similarity score (higher is closer for cosine similarity).
    pub score: f64,
}

#[derive(serde::Deserialize)]
struct NodeEmbeddingVectorNodeHitDb {
    node: surrealdb::sql::Thing,
    score: f64,
}

struct NodeEmbeddingVectorNodeHit {
    node_id: RecordId,
    score: f64,
}

impl TryFrom<NodeEmbeddingVectorNodeHitDb> for NodeEmbeddingVectorNodeHit {
    type Error = StorageError;

    fn try_from(db: NodeEmbeddingVectorNodeHitDb) -> Result<Self, Self::Error> {
        Ok(Self {
            node_id: crate::surreal::record_id_from_thing(&db.node)
                .map_err(|e| StorageError::QueryError(e.to_string()))?,
            score: db.score,
        })
    }
}

#[derive(serde::Deserialize)]
struct NodeRevisionLookupDb {
    id: surrealdb::sql::Thing,
    revision: Option<surrealdb::sql::Thing>,
}

#[derive(serde::Deserialize)]
struct RevisionDocLookupDb {
    id: surrealdb::sql::Thing,
    doc: Option<surrealdb::sql::Thing>,
}

/// Database-only proxy for `NodeEmbedding` to avoid flatten/native enum issues.
#[derive(Deserialize)]
pub(crate) struct NodeEmbeddingDb {
    id: surrealdb::sql::Thing,
    space_id: Option<String>,
    created_at: Option<chrono::DateTime<Utc>>,
    updated_at: Option<chrono::DateTime<Utc>>,
    created_by: Option<String>,
    deleted_at: Option<chrono::DateTime<Utc>>,
    provenance: Option<elephant_types::records::base::Provenance>,
    security: Option<elephant_types::records::base::Security>,
    subjects: Option<Vec<Link<Entity>>>,
    subjects_state: Option<elephant_types::records::base::SubjectsState>,
    node: Option<surrealdb::sql::Thing>,
    model: Option<String>,
    dims: Option<f64>,
    vector: Option<Vec<f32>>,
}

impl TryFrom<NodeEmbeddingDb> for NodeEmbedding {
    type Error = StorageError;

    fn try_from(db: NodeEmbeddingDb) -> Result<Self, Self::Error> {
        Ok(Self {
            base: elephant_types::records::base::BaseFields {
                id: crate::surreal::record_id_from_thing(&db.id)
                    .map_err(|e| StorageError::QueryError(e.to_string()))?,
                space_id: required_field(db.space_id, "space_id")?,
                created_at: db
                    .created_at
                    .ok_or_else(|| StorageError::QueryError("Missing created_at".to_string()))?,
                updated_at: db
                    .updated_at
                    .ok_or_else(|| StorageError::QueryError("Missing updated_at".to_string()))?,
                created_by: required_field(db.created_by, "created_by")?,
                deleted_at: db.deleted_at,
                provenance: required_field(db.provenance, "provenance")?,
                security: required_field(db.security, "security")?,
                subjects: required_field(db.subjects, "subjects")?,
                subjects_state: required_field(db.subjects_state, "subjects_state")?,
            },
            node: Link::new(
                crate::surreal::record_id_from_thing(
                    &db.node
                        .ok_or_else(|| StorageError::QueryError("Missing node link".to_string()))?,
                )
                .map_err(|e| StorageError::QueryError(e.to_string()))?,
            ),
            model: required_field(db.model, "model")?,
            dims: {
                let value = required_field(db.dims, "dims")?;
                let rounded = value.round();
                if (value - rounded).abs() > f64::EPSILON
                    || rounded < f64::from(i32::MIN)
                    || rounded > f64::from(i32::MAX)
                {
                    return Err(StorageError::QueryError(format!(
                        "Invalid dims value: {value}"
                    )));
                }
                #[allow(clippy::cast_possible_truncation)]
                {
                    rounded as i32
                }
            },
            vector: required_field(db.vector, "vector")?,
        })
    }
}

#[async_trait]
impl Repository<NodeEmbedding> for NodeEmbeddingRepository {
    async fn create(&self, record: NodeEmbedding) -> StorageResult<NodeEmbedding> {
        self.store_embedding(record).await
    }

    async fn get(&self, _space_id: &str, _id: &RecordId) -> StorageResult<Option<NodeEmbedding>> {
        Err(StorageError::QueryError(
            "Direct retrieval from NodeEmbeddingRepository requires partition context".to_string(),
        ))
    }

    async fn update(
        &self,
        _space_id: &str,
        _id: &RecordId,
        _record: NodeEmbedding,
    ) -> StorageResult<NodeEmbedding> {
        Err(StorageError::QueryError(
            "Update not supported for embeddings".to_string(),
        ))
    }

    async fn delete(&self, _space_id: &str, _id: &RecordId) -> StorageResult<()> {
        Err(StorageError::QueryError(
            "Delete not supported for embeddings".to_string(),
        ))
    }

    async fn list(
        &self,
        _space_id: &str,
        _limit: usize,
        _cursor: Option<&RecordId>,
    ) -> StorageResult<Vec<NodeEmbedding>> {
        Err(StorageError::QueryError(
            "List not supported for embeddings".to_string(),
        ))
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

    use crate::doc_node_repository::DocNodeRepository;
    use crate::doc_revision_repository::DocRevisionRepository;
    use crate::repository::Repository;
    use elephant_embeddings::{PartitionKey, PartitionManager};
    use elephant_types::records::base::BaseFields;
    use elephant_types::records::doc_node::{NodeRole, TextSpan};
    use elephant_types::records::doc_revision::{DocRevision, RendererMetadata, RevisionStatus};
    use elephant_types::{RecordId, TableName};

    fn make_test_revision(space_id: &str) -> DocRevision {
        DocRevision {
            base: BaseFields::new(
                RecordId::new_v4(TableName::DocRevision),
                space_id.to_string(),
                "tester".to_string(),
            ),
            doc: Link::new(RecordId::new_v4(TableName::Doc)),
            text: "hello world".to_string(),
            text_hash: "hash".to_string(),
            renderer: RendererMetadata {
                name: "test-renderer".to_string(),
                version: "1.0.0".to_string(),
                config_hash: None,
            },
            manifest: None,
            supersedes: None,
            status: RevisionStatus::Active,
            rendered_at: Utc::now(),
        }
    }

    fn make_test_node(space_id: &str, revision_id: &RecordId, key: &str) -> DocNode {
        DocNode {
            base: BaseFields::new(
                RecordId::new_v4(TableName::DocNode),
                space_id.to_string(),
                "tester".to_string(),
            ),
            revision: Link::new(revision_id.clone()),
            role: NodeRole::Chunk,
            span: TextSpan::new(0, 5),
            node_key: key.to_string(),
            text: Some("hello".to_string()),
            parent: None,
            position: 0,
            depth: 0,
            embedding_model: None,
            collection: None,
        }
    }

    fn make_test_embedding(space_id: &str, node_id: &RecordId) -> NodeEmbedding {
        NodeEmbedding {
            base: BaseFields::new(
                RecordId::new_v4(TableName::DocNode),
                space_id.to_string(),
                "tester".to_string(),
            ),
            node: Link::new(node_id.clone()),
            model: "test-model".to_string(),
            dims: 3,
            vector: vec![1.0, 0.0, 0.0],
        }
    }

    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep revision_repo, node_repo, repo in the original fixture scope through assertions and cleanup."
    )]
    async fn exercise_partition_vector_search(client: StorageClient) {
        let revision_repo = DocRevisionRepository::new(client.clone());
        let node_repo = DocNodeRepository::new(client.clone());
        let repo = NodeEmbeddingRepository::new(client, PartitionManager::new());
        let key = PartitionKey::new(
            "default".to_string(),
            "L0".to_string(),
            "test-model".to_string(),
            3,
        )
        .expect("partition key");
        let query = [0.9_f32, 0.1, 0.0];
        let fixtures = [
            (
                "space",
                "alpha",
                [1.0, 0.0, 0.0],
                "default",
                "test-model",
                false,
            ),
            (
                "space",
                "beta",
                [0.0, 1.0, 0.0],
                "default",
                "test-model",
                false,
            ),
            (
                "other-space",
                "alpha",
                query,
                "default",
                "test-model",
                false,
            ),
            ("space", "alpha", query, "other-policy", "test-model", false),
            ("space", "alpha", query, "default", "other-model", false),
            ("space", "alpha", query, "default", "test-model", true),
        ];
        let mut expected = Vec::new();
        for (space, collection, vector, policy, model, deleted) in fixtures {
            // Use the actual write path so vector_norm and revision-to-doc
            // resolution match records produced by the embedding pipeline.
            let revision = revision_repo
                .create(make_test_revision(space))
                .await
                .expect("create revision");
            let mut node = make_test_node(space, &revision.base.id, "vector-node");
            node.collection = Some(collection.to_string());
            let node = node_repo.create(node).await.expect("create node");
            let mut embedding = make_test_embedding(space, &node.base.id);
            embedding.vector = vector.to_vec();
            embedding.base.security.policy_id = policy.to_string();
            embedding.model = model.to_string();
            if deleted {
                embedding.base.soft_delete();
            }
            repo.store_embedding(embedding)
                .await
                .expect("store embedding");
            expected.push((node.base.id, revision.doc.id().clone()));
        }

        let hits = repo
            .search_partition(&key, "space", &query, 1, None, false)
            .await
            .expect("nearest search");
        assert_eq!(hits.len(), 1);
        assert_eq!(
            (&hits[0].node_id, &hits[0].doc_id),
            (&expected[0].0, &expected[0].1)
        );
        assert!((hits[0].score - 0.993_883_7).abs() < 0.000_001);

        let hits = repo
            .search_partition(&key, "space", &query, 10, None, false)
            .await
            .expect("isolated search");
        assert_eq!(
            hits.len(),
            2,
            "other spaces, partitions, and deleted rows are excluded"
        );
        assert_eq!(hits[0].node_id, expected[0].0);
        assert_eq!(hits[1].node_id, expected[1].0);
        assert!(hits[0].score > hits[1].score);

        let hits = repo
            .search_partition(&key, "space", &query, 10, Some("beta"), false)
            .await
            .expect("collection search");
        assert_eq!(hits.len(), 1);
        assert_eq!(
            (&hits[0].node_id, &hits[0].doc_id),
            (&expected[1].0, &expected[1].1)
        );

        let hits = repo
            .search_partition(&key, "space", &query, 10, None, true)
            .await
            .expect("search including deleted embeddings");
        assert_eq!(hits.len(), 3);
        assert_eq!(hits[0].node_id, expected[5].0);
        assert!((hits[0].score - 1.0).abs() < 0.000_001);
    }

    #[test]
    fn test_semantic_window_settles_only_when_unseen_chunks_cannot_win() {
        use super::semantic_window_settled as settled;
        // One partition, window full: k docs settle it.
        assert!(settled(10, 10, Some(0.80), Some(0.80)));
        // Two partitions: P1's window is filled by three long docs scoring
        // >= 0.80, P2 adds seven docs near 0.5. The 10th visible doc scores
        // 0.5 but P1 may still hold a 0.79 doc: widen.
        assert!(!settled(10, 10, Some(0.5), Some(0.80)));
        // No partition filled its window: everything was seen.
        assert!(settled(10, 10, Some(0.1), None));
        // Too few visible docs while a window is full: widen.
        assert!(!settled(3, 10, None, Some(0.2)));
        assert!(settled(0, 0, None, Some(0.9)));
    }

    #[test]
    fn test_window_fullness_and_floor_come_from_the_raw_rows() {
        use super::PartitionWindow;
        let hit = |score| NodeEmbeddingVectorHit {
            node_id: RecordId::new_v4(TableName::DocNode),
            doc_id: RecordId::new_v4(TableName::Doc),
            score,
        };
        // Three raw rows, one orphan dropped: still a full window of 3.
        let window = PartitionWindow::from_raw(vec![hit(0.9), hit(0.7)], &[0.9, 0.8, 0.7], 3);
        assert!(window.full);
        assert_eq!(window.floor, Some(0.7));
        assert_eq!(window.raise_floor(Some(0.75)), Some(0.75));
        assert_eq!(window.raise_floor(Some(0.6)), Some(0.7));
        let short = PartitionWindow::from_raw(vec![hit(0.9)], &[0.9], 3);
        assert!(!short.full);
        assert_eq!(short.raise_floor(Some(0.5)), Some(0.5));
        assert!(!PartitionWindow::from_raw(Vec::new(), &[], 0).full);
    }

    #[tokio::test]
    async fn filtered_hnsw_twins_do_not_consume_any_requested_result_slots() {
        let client = StorageClient::new_memory().await.expect("memory");
        let key =
            PartitionKey::new("default".into(), "L0".into(), "twins".into(), 3).expect("partition");
        let table = key.table_name();
        client
            .query_checked(&PartitionManager::create_partition_ddl(&key))
            .await
            .expect("ddl");
        // More ineligible twins than the old fixed search overfetch allowance.
        for index in 0..24 {
            client.query_checked(&format!("CREATE {table}:ineligible{index} SET vector=[1,0,0], space_id='foreign', vector_norm=1")).await.expect("foreign twin");
        }
        for index in 0..3 {
            client.query_checked(&format!("CREATE {table}:eligible{index} SET vector=[1,0,0], space_id='selected', vector_norm=1")).await.expect("eligible twin");
        }
        // Every duplicate membership, including the in-place bitmap range,
        // must reach the durable index before predicates are considered.
        let mut response = client
            .query_checked(&format!(
                "SELECT VALUE id FROM {table} WHERE vector <|27,100|> [1,0,0]"
            ))
            .await
            .expect("all indexed twins");
        let all_ids: Vec<surrealdb::sql::Thing> = response.take(0).expect("all record ids");
        assert_eq!(all_ids.len(), 27, "bitmap inserts must persist");
        for limit in [1, 2, 3] {
            let query = format!("SELECT VALUE id FROM {table} WHERE vector <|{limit},100|> [1,0,0] AND space_id='selected' AND deleted_at=NONE");
            let mut response = client.query_checked(&query).await.expect("indexed search");
            let ids: Vec<surrealdb::sql::Thing> = response.take(0).expect("record ids");
            assert_eq!(ids.len(), limit);
            assert!(ids
                .iter()
                .all(|id| id.to_string().contains("eligible")
                    && !id.to_string().contains("ineligible")));
        }
        client
            .query_checked(&format!(
                "UPDATE {table} SET deleted_at=time::now() WHERE space_id='selected'"
            ))
            .await
            .expect("delete eligible rows");
        let mut response = client.query_checked(&format!("SELECT VALUE id FROM {table} WHERE vector <|1,100|> [1,0,0] AND space_id='selected' AND deleted_at=NONE")).await.expect("all denied search");
        let ids: Vec<surrealdb::sql::Thing> = response.take(0).expect("empty result");
        assert!(ids.is_empty(), "filter must remain fail closed");
        // Physical deletion exercises bitmap removal above eight entries and
        // its transition back to the small representation. An unfiltered KNN
        // query must not spend any of its three slots on stale foreign IDs.
        client
            .query_checked(&format!("DELETE {table} WHERE space_id='foreign'"))
            .await
            .expect("remove foreign twins");
        client
            .query_checked(&format!(
                "UPDATE {table} SET deleted_at=NONE WHERE space_id='selected'"
            ))
            .await
            .expect("restore selected fixture rows");
        let mut response = client
            .query_checked(&format!(
                "SELECT VALUE id FROM {table} WHERE vector <|3,100|> [1,0,0]"
            ))
            .await
            .expect("surviving indexed twins");
        drop(client);
        let survivors: Vec<surrealdb::sql::Thing> = response.take(0).expect("survivor ids");
        assert_eq!(survivors.len(), 3, "bitmap removals must persist");
        assert!(survivors.iter().all(
            |id| id.to_string().contains("eligible") && !id.to_string().contains("ineligible")
        ));
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep client in the original fixture scope through assertions and cleanup."
    )]
    async fn test_partition_vector_search_plan_uses_the_hnsw_index() {
        let client = StorageClient::new_memory()
            .await
            .expect("SurrealDB memory client");
        let key = PartitionKey::new(
            "default".to_string(),
            "L0".to_string(),
            "test-model".to_string(),
            3,
        )
        .expect("partition key");
        let table = key.table_name();
        client
            .query_checked(&PartitionManager::create_partition_ddl(&key))
            .await
            .expect("partition ddl");
        let conditions = vec![
            "space_id = $space_id".to_string(),
            "deleted_at IS NONE".to_string(),
            "vector_norm > 0".to_string(),
        ];
        let query = super::partition_knn_query(&table, &conditions, 10);
        let mut response = client
            .query_bind(
                &format!("{query} EXPLAIN"),
                crate::bindings! {
                    "space_id" => "space",
                    "vector" => surrealdb::sql::Value::Array(surrealdb::sql::Array::from(vec![1.0_f32, 0.0, 0.0]))
                },
            )
            .await
            .expect("explain partition search");
        let plan: Vec<serde_json::Value> = response.take(0).expect("explain projection");
        let rendered = serde_json::to_string(&plan).expect("render plan");
        println!("partition search plan: {rendered}");
        assert!(
            !rendered.contains("Iterate Table"),
            "partition search scans the table instead of the HNSW index: {rendered}"
        );
        assert!(
            rendered.contains(&format!("{table}_hnsw")),
            "partition search does not name the partition HNSW index: {rendered}"
        );
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep client in the original fixture scope through assertions and cleanup."
    )]
    async fn test_hit_lookups_fetch_records_without_scanning_tables() {
        let client = StorageClient::new_memory()
            .await
            .expect("SurrealDB memory client");
        let node_ids = vec![
            RecordId::new_v4(TableName::DocNode),
            RecordId::new_v4(TableName::DocNode),
        ];
        let revision_ids = vec![RecordId::new_v4(TableName::DocRevision)];
        for (query, ids) in [
            (super::NODE_REVISION_LOOKUP, &node_ids),
            (super::REVISION_DOC_LOOKUP, &revision_ids),
        ] {
            let things: Vec<surrealdb::sql::Value> = ids
                .iter()
                .map(crate::surreal::thing_from_record_id)
                .map(surrealdb::sql::Value::from)
                .collect();
            let mut response = client
                .query_bind(
                    &format!("{query} EXPLAIN"),
                    crate::bindings! {
                        "ids" => surrealdb::sql::Value::Array(surrealdb::sql::Array::from(things))
                    },
                )
                .await
                .expect("explain lookup");
            // The plan holds record ids, which serde_json cannot represent;
            // read it as a SurrealDB value and check its rendering.
            let plan: surrealdb::Value = response.take(0).expect("explain projection");
            let rendered = format!("{plan:?}");
            assert!(
                rendered.contains("Iterate"),
                "unexpected plan shape for {query}: {rendered}"
            );
            assert!(
                !rendered.contains("Iterate Table"),
                "lookup scans a table instead of fetching records: {query} -> {rendered}"
            );
        }
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep client, repo in the original fixture scope through assertions and cleanup."
    )]
    async fn test_orphan_rows_still_count_toward_a_full_window() {
        let client = StorageClient::new_memory()
            .await
            .expect("SurrealDB memory client");
        let repo = NodeEmbeddingRepository::new(client, PartitionManager::new());
        let key = PartitionKey::new(
            "default".to_string(),
            "L0".to_string(),
            "test-model".to_string(),
            3,
        )
        .expect("partition key");
        // An embedding whose node was never created: it can rank first but
        // resolves to no document.
        let orphan_node = RecordId::new_v4(TableName::DocNode);
        let mut embedding = make_test_embedding("space", &orphan_node);
        embedding.vector = vec![1.0, 0.0, 0.0];
        repo.store_embedding(embedding).await.expect("store orphan");

        let window = repo
            .search_partition_window(&key, "space", &[1.0, 0.0, 0.0], 1, None, false)
            .await
            .expect("window");
        assert!(window.hits.is_empty(), "the orphan resolves to no document");
        assert!(
            window.full,
            "but the raw window was full: more may lie beyond it"
        );
        assert!(window.floor.is_some());
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep client, revision_repo, node_repo, repo in the original fixture scope through assertions and cleanup."
    )]
    async fn test_fake_embedding_purge_resets_only_ramp_vectors_once() {
        use elephant_embeddings::{EmbeddingProvider, FakeEmbeddingProvider};

        let client = StorageClient::new_memory()
            .await
            .expect("SurrealDB memory client");
        let revision_repo = DocRevisionRepository::new(client.clone());
        let node_repo = DocNodeRepository::new(client.clone());
        let repo = NodeEmbeddingRepository::new(client.clone(), PartitionManager::new());
        let dims = 1200;
        let ramp = FakeEmbeddingProvider::new("text-embedding-3-large", dims)
            .embed(&["some chunk text".to_string()])
            .await
            .expect("fake vector")
            .remove(0);
        let real: Vec<f32> = (0..u16::try_from(dims).expect("dims"))
            .map(|i| (f32::from(i) * 0.37).sin() * 0.03)
            .collect();

        let mut node_ids = Vec::new();
        for (key, vector) in [("fake", ramp), ("real", real)] {
            let revision = revision_repo
                .create(make_test_revision("space"))
                .await
                .expect("revision");
            let mut node = make_test_node("space", &revision.base.id, key);
            node.embedding_model = Some("text-embedding-3-large".to_string());
            let node = node_repo.create(node).await.expect("node");
            let mut embedding = make_test_embedding("space", &node.base.id);
            embedding.model = "text-embedding-3-large".to_string();
            embedding.dims = i32::try_from(dims).expect("dims");
            embedding.vector = vector;
            repo.store_embedding(embedding).await.expect("store");
            node_ids.push(node.base.id);
        }

        let report = crate::fake_embedding_repair::purge_fake_embeddings(&client)
            .await
            .expect("purge");
        assert!(!report.skipped);
        assert_eq!(
            report.partitions.iter().map(|(_, n)| n).sum::<usize>(),
            1,
            "{report:?}"
        );

        let fake_node = node_repo
            .get("space", &node_ids[0])
            .await
            .expect("get")
            .expect("fake node");
        assert!(
            fake_node.embedding_model.is_none(),
            "fake node re-queued for embedding"
        );
        let real_node = node_repo
            .get("space", &node_ids[1])
            .await
            .expect("get")
            .expect("real node");
        assert_eq!(
            real_node.embedding_model.as_deref(),
            Some("text-embedding-3-large")
        );

        let again = crate::fake_embedding_repair::purge_fake_embeddings(&client)
            .await
            .expect("second purge");
        assert!(again.skipped, "one-shot: the marker stops a rescan");
    }

    #[tokio::test]
    async fn test_partition_vector_search_in_surreal_memory() {
        exercise_partition_vector_search(
            StorageClient::new_memory()
                .await
                .expect("SurrealDB memory client"),
        )
        .await;
    }

    #[tokio::test]
    async fn test_partition_vector_search_in_embedded_memory() {
        exercise_partition_vector_search(
            StorageClient::new_embedded()
                .await
                .expect("embedded client"),
        )
        .await;
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep client, revision_repo, node_repo, repo in the original fixture scope through assertions and cleanup."
    )]
    async fn test_store_embeddings_and_mark_nodes_rejects_mismatched_node_ids() {
        let client = crate::StorageClient::new_embedded()
            .await
            .expect("embedded client");
        let revision_repo = DocRevisionRepository::new(client.clone());
        let node_repo = DocNodeRepository::new(client.clone());
        let repo = NodeEmbeddingRepository::new(client, PartitionManager::new());

        let revision = revision_repo
            .create(make_test_revision("default"))
            .await
            .expect("create revision");
        let first = node_repo
            .create(make_test_node("default", &revision.base.id, "n1"))
            .await
            .expect("create first node");
        let second = node_repo
            .create(make_test_node("default", &revision.base.id, "n2"))
            .await
            .expect("create second node");

        let err = repo
            .store_embeddings_and_mark_nodes(
                "default",
                std::slice::from_ref(&first.base.id),
                "test-model",
                Utc::now(),
                vec![make_test_embedding("default", &second.base.id)],
            )
            .await
            .expect_err("mismatched batch should fail");
        assert!(err.to_string().contains("Embedding batch mismatch"));

        let first_after = node_repo
            .get("default", &first.base.id)
            .await
            .expect("load first node")
            .expect("first node exists");
        assert!(first_after.embedding_model.is_none());
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep client, revision_repo, node_repo, repo in the original fixture scope through assertions and cleanup."
    )]
    async fn test_store_embeddings_and_mark_nodes_rejects_space_mismatch() {
        let client = crate::StorageClient::new_embedded()
            .await
            .expect("embedded client");
        let revision_repo = DocRevisionRepository::new(client.clone());
        let node_repo = DocNodeRepository::new(client.clone());
        let repo = NodeEmbeddingRepository::new(client, PartitionManager::new());

        let revision = revision_repo
            .create(make_test_revision("default"))
            .await
            .expect("create revision");
        let node = node_repo
            .create(make_test_node("default", &revision.base.id, "n1"))
            .await
            .expect("create node");

        let err = repo
            .store_embeddings_and_mark_nodes(
                "default",
                std::slice::from_ref(&node.base.id),
                "test-model",
                Utc::now(),
                vec![make_test_embedding("other-space", &node.base.id)],
            )
            .await
            .expect_err("space-mismatched batch should fail");
        assert!(err.to_string().contains("space mismatch"));

        let node_after = node_repo
            .get("default", &node.base.id)
            .await
            .expect("load node")
            .expect("node exists");
        assert!(node_after.embedding_model.is_none());
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep client, revision_repo, node_repo, repo in the original fixture scope through assertions and cleanup."
    )]
    async fn test_store_embeddings_and_mark_nodes_rejects_model_mismatch() {
        let client = crate::StorageClient::new_embedded()
            .await
            .expect("embedded client");
        let revision_repo = DocRevisionRepository::new(client.clone());
        let node_repo = DocNodeRepository::new(client.clone());
        let repo = NodeEmbeddingRepository::new(client, PartitionManager::new());

        let revision = revision_repo
            .create(make_test_revision("default"))
            .await
            .expect("create revision");
        let node = node_repo
            .create(make_test_node("default", &revision.base.id, "n1"))
            .await
            .expect("create node");

        let mut embedding = make_test_embedding("default", &node.base.id);
        embedding.model = "other-model".to_string();

        let err = repo
            .store_embeddings_and_mark_nodes(
                "default",
                std::slice::from_ref(&node.base.id),
                "test-model",
                Utc::now(),
                vec![embedding],
            )
            .await
            .expect_err("model-mismatched batch should fail");
        assert!(err.to_string().contains("model mismatch"));

        let node_after = node_repo
            .get("default", &node.base.id)
            .await
            .expect("load node")
            .expect("node exists");
        assert!(node_after.embedding_model.is_none());
    }
    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep client, repository in the original fixture scope through assertions and cleanup."
    )]
    async fn embedded_staging_keeps_distinct_profiles_for_the_same_node() {
        let client = StorageClient::new_embedded().await.unwrap();
        let repository = NodeEmbeddingRepository::new(client.clone(), PartitionManager::new());
        let node_id = RecordId::new_v4(TableName::DocNode);
        let mut original = make_test_embedding("space", &node_id);
        original.model = "recall:old-profile".into();
        let mut replacement = original.clone();
        replacement.model = "recall:new-profile".into();
        replacement.vector = vec![0.0, 1.0, 0.0];
        repository.store_embedding(original.clone()).await.unwrap();
        repository.store_embedding(replacement).await.unwrap();
        let rows = client
            .embedded_store()
            .unwrap()
            .scan::<NodeEmbedding>()
            .await
            .unwrap();
        assert_eq!(rows.len(), 2);
        assert!(rows
            .iter()
            .any(|row| row.model == original.model && row.vector == original.vector));
        assert_ne!(rows[0].base.id, rows[1].base.id);
        assert_eq!(rows[0].node.id(), rows[1].node.id());
    }
}
